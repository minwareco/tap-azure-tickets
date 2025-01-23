import hashlib
import os
import json
import functools
import sys
from dateutil import parser
from contextlib import suppress
import pytz
import time
import requests
import re
import singer
import singer.bookmarks as bookmarks
import singer.metrics as metrics
import backoff

from singer import metadata

session = requests.Session()
logger = singer.get_logger()

REQUIRED_CONFIG_KEYS = [
    'start_date',
    'user_name',
    'access_token',
    'org',
    'projects'
]

KEY_PROPERTIES = {
    'boards': ['id'],
    'iterations': ['id'],
    'projects': ['id'],
    'teams': ['id'],
    'teammembers': ['id'],
    'updates': ['id'],
    'workitems': ['id'],
    'workitemtypes': ['id'],
    'workitemtypecategories': ['id'],
}

API_VERSION = "6.0"

class AzureException(Exception):
    pass

class BadCredentialsException(AzureException):
    pass

class AuthException(AzureException):
    pass

class NotFoundException(AzureException):
    pass

class BadRequestException(AzureException):
    pass

class InternalServerError(AzureException):
    pass

class UnprocessableError(AzureException):
    pass

class NotModifiedError(AzureException):
    pass

class MovedPermanentlyError(AzureException):
    pass

class ConflictError(AzureException):
    pass

class RateLimitExceeded(AzureException):
    pass

ERROR_CODE_EXCEPTION_MAPPING = {
    301: {
        "raise_exception": MovedPermanentlyError,
        "message": "The resource you are looking for is moved to another URL."
    },
    304: {
        "raise_exception": NotModifiedError,
        "message": "The requested resource has not been modified since the last time you accessed it."
    },
    400:{
        "raise_exception": BadRequestException,
        "message": "The request is missing or has a bad parameter."
    },
    401: {
        "raise_exception": BadCredentialsException,
        "message": "Invalid authorization credentials. Please check that your access token is " \
            "correct, has not expired, and has read access to the 'Code' and 'Pull Request Threads' scopes."
    },
    403: {
        "raise_exception": AuthException,
        "message": "User doesn't have permission to access the resource."
    },
    404: {
        "raise_exception": NotFoundException,
        "message": "The resource you have specified cannot be found"
    },
    409: {
        "raise_exception": ConflictError,
        "message": "The request could not be completed due to a conflict with the current state of the server."
    },
    422: {
        "raise_exception": UnprocessableError,
        "message": "The request was not able to process right now."
    },
    429: {
        "raise_exception": RateLimitExceeded,
        "message": "Request rate limit exceeded."
    },
    500: {
        "raise_exception": InternalServerError,
        "message": "An error has occurred at Azure's end processing this request."
    },
    502: {
        "raise_exception": InternalServerError,
        "message": "Azure's service is not currently available."
    },
    503: {
        "raise_exception": InternalServerError,
        "message": "Azure's service is not currently available."
    },
    504: {
        "raise_exception": InternalServerError,
        "message": "Azure's service is not currently available."
    },
}

def get_bookmark(state, project_id, stream_name, bookmark_key, default_value=None):
    project_stream_dict = bookmarks.get_bookmark(state, project_id, stream_name)
    if project_stream_dict:
        return project_stream_dict.get(bookmark_key)
    if default_value:
        return default_value
    return None

default_bookmark_date = '1970-01-01T00:00:00Z'

def get_bookmark_date(state, project_id, stream_name, bookmark_key, default_value=default_bookmark_date):
    bookmark = get_bookmark(state, project_id, stream_name, bookmark_key, default_value)
    if not bookmark:
        bookmark = default_bookmark_date if default_value is None else default_value

    return parse_iso8601(bookmark)

def parse_iso8601(input):
    result = parser.parse(input)
    if result.tzinfo is None:
        return pytz.UTC.localize(result)
    else:
        return result


def raise_for_error(resp, source, url):
    error_code = resp.status_code
    try:
        response_json = resp.json()
    except Exception:
        response_json = {}

    # TODO: if/when we hook this up to exception tracking, report the URL as metadat rather than as
    # part of the exception message.

    if error_code == 404:
        details = ERROR_CODE_EXCEPTION_MAPPING.get(error_code).get("message")
        message = "HTTP-error-code: 404, Error: {}. Please check that the following URL is valid "\
            "and you have permission to access it: {}".format(details, url)
    else:
        message = "HTTP-error-code: {}, Error: {} Url: {}".format(
            error_code, ERROR_CODE_EXCEPTION_MAPPING.get(error_code, {}) \
            .get("message", "Unknown Error") if response_json == {} else response_json, \
            url)

        # Map HTTP version numbers
        http_versions = {
            10: "HTTP/1.0",
            11: "HTTP/1.1",
            20: "HTTP/2.0"
        }
        http_version = http_versions.get(resp.raw.version, "Unknown")
        message += "\nResponse Details: \n\t{} {}".format(http_version, resp.status_code)
        for key, value in resp.headers.items():
            message += "\n\t{}: {}".format(key, value)
        if resp.content:
            message += "\n\t{}".format(resp.text)

    exc = ERROR_CODE_EXCEPTION_MAPPING.get(error_code, {}).get("raise_exception", AzureException)
    raise exc(message) from None

# Ignore specific exception
# This exception occurs when the permissions don't allow access to a specific board
# There is no simple way to verify access before this occurs
ignored_exceptions = [
    'TF400497: The backlog iteration path that you specified is no longer valid.',
    'TF400499: You have not set your team field.'
]

def request(source, url, method='GET', json=None):
    """
    This function performs an HTTP request and implements a robust retry mechanism
    to handle transient errors and optimize the request's success rate.

    Key Features:
    1. **Retry on Predicate (Retry-After Header)**:
    - Retries when the response contains a "Retry-After" header, which is commonly used
        to indicate when a client should retry a request.
    - This is particularly useful for Azure, which may include "Retry-After" headers
        for both 429 (Too Many Requests) and 5xx (Server Error) responses.
    - A maximum of 8 retry attempts is made, respecting the "Retry-After" value
        specified in the header.

    2. **Retry on Exceptions**:
    - Retries when a `requests.exceptions.RequestException` occurs.
    - Uses an exponential backoff strategy (doubling the delay between retries)
        with a maximum of 5 retry attempts.
    - Stops retrying if the exception is due to a client-side error (HTTP 4xx),
        as these are typically non-recoverable. The single exception is 429 (Too Many Requests).

    Parameters:
    - `source` (str): The source that is triggering the HTTP request.
    - `url` (str): The URL to which the HTTP request is sent.
    - `method` (str, optional): The HTTP method to use (default is 'GET').

    Returns:
    - `response` (requests.Response): The HTTP response object.

    Notes:
    - This function leverages the `backoff` library for retry strategies and logging.
    - A session object (assumed to be pre-configured) is used for making the HTTP request.
    """

    exponential_factor = 5
    tries = 0
    def backoff_value(response):
        nonlocal tries
        with suppress(TypeError, ValueError, AttributeError):
            return int(response.headers.get("Retry-After"))
        backoff_time = exponential_factor * (2 ** tries)
        tries += 1
        return backoff_time

    def execute_request(source, url, method='GET', json=None):
        with metrics.http_request_timer(source) as timer:
            timer.tags['url'] = url

            response = session.request(method=method, url=url, json=json)

            timer.tags[metrics.Tag.http_status_code] = response.status_code
            timer.tags['header_retry-after'] = response.headers.get('retry-after')
            timer.tags['header_x-ratelimit-resource'] = response.headers.get('x-ratelimit-resource')
            timer.tags['header_x-ratelimit-delay'] = response.headers.get('x-ratelimit-delay')
            timer.tags['header_x-ratelimit-limit'] = response.headers.get('x-ratelimit-limit')
            timer.tags['header_x-ratelimit-remaining'] = response.headers.get('x-ratelimit-remaining')
            timer.tags['header_x-ratelimit-reset'] = response.headers.get('x-ratelimit-reset')

            return response

    backoff_on_exception = backoff.on_exception(backoff.expo,
                      (requests.exceptions.RequestException),
                      max_tries=7,
                      max_time=3600,
                      giveup=lambda e: e.response is not None and e.response.status_code != 429 and 400 <= e.response.status_code < 500,
                      factor=exponential_factor,
                      jitter=backoff.random_jitter,
                      logger=logger)

    backoff_on_predicate = backoff.on_predicate(backoff.runtime,
                      predicate=lambda r:
                        r.headers.get("Retry-After", None) != None or (
                            r.status_code >= 300 and not
                            any(ignored_ex in r.text for ignored_ex in ignored_exceptions)
                        ),
                      max_tries=7,
                      max_time=3600,
                      value=backoff_value,
                      jitter=backoff.random_jitter,
                      logger=logger)

    return backoff_on_exception(backoff_on_predicate(execute_request))(source, url, method, json)

# pylint: disable=dangerous-default-value
def authed_get(source, url, headers={}):
    session.headers.update(headers)
    # Uncomment for debugging
    #logger.info("requesting {}".format(url))
    resp = request(source, url, method='get')

    if resp.status_code != 200:
        raise_for_error(resp, source, url)
    return resp

# pylint: disable=dangerous-default-value
def authed_post(source, url, json, headers={}):
    session.headers.update(headers)
    # Uncomment for debugging
    #logger.info("requesting {}".format(url))
    resp = request(source, url, method='post', json=json)

    if resp.status_code != 200:
        raise_for_error(resp, source, url)

    return resp

PAGE_SIZE = 100
def authed_get_all_pages(source, url, page_param_name='', skip_param_name='',
        no_stop_indicator=False, iterate_state=None, headers={}):
    offset = 0
    if iterate_state and 'offset' in iterate_state:
        offset = iterate_state['offset']
    if page_param_name:
        baseurl = url + '&{}={}'.format(page_param_name, PAGE_SIZE)
    else:
        baseurl = url
    continuationToken = ''
    if iterate_state and 'continuationToken' in iterate_state:
        continuationToken = iterate_state['continuationToken']
    isDone = False
    while True:
        if skip_param_name == 'continuationToken':
            if continuationToken:
                cururl = baseurl + '&continuationToken={}'.format(continuationToken)
            else:
                cururl = baseurl
        elif page_param_name:
            cururl = baseurl + '&{}={}'.format(skip_param_name, offset)
        else:
            cururl = baseurl

        r = authed_get(source, cururl, headers)
        yield r

        # Look for a link header, and will have to update the URL parameters accordingly
        # link: <https://dev.azure.com/_apis/git/repositories/scheduled/commits>;rel="next"
        # Exception: for the changes endpoint, the server sends an empty link header, which just
        # seems buggy, but we have to handle it.
        if page_param_name and 'link' in r.headers and \
                ('rel="next"' in r.headers['link'] or '' == r.headers['link']):
            offset += PAGE_SIZE
        # You know what's awesome? How every endpoint implements pagination in a completely
        # different and inconsistent way.
        # Funny true story: After I wrote the comment above, I discovered that the pullrequests
        # endpoint actually has NO method of indicating the availability of more results, so you
        # literally have to just keep querying until you don't get any more data.
        elif no_stop_indicator:
            if r.json()['count'] < PAGE_SIZE:
                isDone = True
                break
            else:
                offset += PAGE_SIZE
        elif 'x-ms-continuationtoken' in r.headers:
            continuationToken = r.headers['x-ms-continuationtoken']
        else:
            isDone = True
            break
        # If there's an iteration state, we only want to do one iteration
        if iterate_state:
            break

    # Populate the iterate state if it's present
    if iterate_state:
        iterate_state['stop'] = isDone
        iterate_state['offset'] = offset
        iterate_state['continuationToken'] = continuationToken

def get_abs_path(path):
    return os.path.join(os.path.dirname(os.path.realpath(__file__)), path)

def load_schemas():
    schemas = {}

    for filename in os.listdir(get_abs_path('schemas')):
        path = get_abs_path('schemas') + '/' + filename
        file_raw = filename.replace('.json', '')
        with open(path) as file:
            schema = json.load(file)
            refs = schema.pop("definitions", {})
            if refs:
                singer.resolve_schema_references(schema, refs)
            schemas[file_raw] = schema

    return schemas

class DependencyException(Exception):
    pass

def validate_dependencies(selected_stream_ids):
    errs = []
    msg_tmpl = ("Unable to extract '{0}' data, "
                "to receive '{0}' data, you also need to select '{1}'.")

    for main_stream, sub_streams in SUB_STREAMS.items():
        if main_stream not in selected_stream_ids:
            for sub_stream in sub_streams:
                if sub_stream in selected_stream_ids:
                    errs.append(msg_tmpl.format(sub_stream, main_stream))

    if errs:
        raise DependencyException(" ".join(errs))


def write_metadata(mdata, values, breadcrumb):
    mdata.append(
        {
            'metadata': values,
            'breadcrumb': breadcrumb
        }
    )

def populate_metadata(schema_name, schema):
    mdata = metadata.new()
    #mdata = metadata.write(mdata, (), 'forced-replication-method', KEY_PROPERTIES[schema_name])
    mdata = metadata.write(mdata, (), 'table-key-properties', KEY_PROPERTIES[schema_name])

    for field_name in schema['properties'].keys():
        if field_name in KEY_PROPERTIES[schema_name]:
            mdata = metadata.write(mdata, ('properties', field_name), 'inclusion', 'automatic')
        else:
            mdata = metadata.write(mdata, ('properties', field_name), 'inclusion', 'available')

    return mdata

def get_catalog():
    raw_schemas = load_schemas()
    streams = []

    for schema_name, schema in raw_schemas.items():

        # get metadata for each field
        mdata = populate_metadata(schema_name, schema)

        # create and add catalog entry
        catalog_entry = {
            'stream': schema_name,
            'tap_stream_id': schema_name,
            'schema': schema,
            'metadata' : metadata.to_list(mdata),
            'key_properties': KEY_PROPERTIES[schema_name],
        }
        streams.append(catalog_entry)

    return {'streams': streams}

def do_discover(config):
    catalog = get_catalog()
    # dump catalog
    print(json.dumps(catalog, indent=2))

def get_selected_projects(org, filters):
    filters = filters or []

    # convert simple wildcard filter syntax into regex. for example, "abc*" becomes "^abc.*$"
    for filter in filters:
        filter = filter.strip() \
            .replace('.', '\\.') \
            .replace('*', '.*')
        filter = "^{}$".format(filter)

    # if there are no filters, create one which will allow all projects
    if len(filters) == 0:
        filters = ['.*']

    result = []
    for response in authed_get_all_pages(
        'projects',
        "https://dev.azure.com/{}/_apis/projects?api-version={}".format(org, API_VERSION),
        '$top',
        'continuationToken'
    ):
        projects = response.json()['value']
        for project in projects:
            for filter in filters:
                if re.search(filter, project['name']):
                    result.append(project)
    return result

def get_teams_for_project(org, projectId):
    teams = []
    for response in authed_get_all_pages(
        'teams',
        "https://dev.azure.com/{}/_apis/projects/{}/teams?api-version={}&$mine=false".format(org, projectId, API_VERSION),
        '$top',
        'continuationToken'
    ):
        teams += response.json()['value']

    return teams

def translateObject(object, org, projectId):
    objectCopy = object.copy()
    objectCopy['org'] = org
    objectCopy['project'] = projectId
    return {
        'id': object['id'] if 'id' in object else object['url'],
        'object': json.dumps(objectCopy),
    }

def emit_records(streamId, schema, org, project, objects, extraction_time, counter, mdata):
    mdataMap = metadata.to_map(mdata)
    for object in objects:
        with singer.Transformer() as transformer:
            rec = transformer.transform(object, schema, metadata=mdataMap)
        singer.write_record(streamId, rec, time_extracted=extraction_time)
        if counter:
            counter.increment()

# Skip emitting data if the whole response is exactly the same as last time
def checkOutputState(outputItems, state, streamId, projectId):
    dataHash = hashlib.md5(json.dumps(outputItems).encode()).hexdigest()

    bookmark = bookmarks.get_bookmark(state, projectId, streamId)
    if bookmark:
        existingHash = bookmark.get('hash')
    else:
        existingHash = None

    # Not changed, we're done
    if existingHash == dataHash:
        return None

    return dataHash

def emit_bookmark_records(streamId, schema, org, project, objects, extraction_time, counter, mdata, state, bookmarkId):
    bookmarkHash = checkOutputState(objects, state, bookmarkId, project['id'])
    if bookmarkHash:
        emit_records(streamId, schema, org, project, objects, extraction_time, counter, mdata)
        singer.write_bookmark(state, project['id'], bookmarkId, {
            'hash': bookmarkHash
        })
        singer.write_state(state)

def sync_all_boards(schema, org, project, teams, state, mdata, start_date):
    # bookmarks not used for this stream
    streamId = 'boards'
    logger.info("Syncing all boards")

    with metrics.record_counter(streamId) as counter:
        extraction_time = singer.utils.now()
        try:
            pageNum = 0
            for response in authed_get_all_pages(
                'boards',
                "https://dev.azure.com/{}/{}/_apis/work/boards?api-version={}".format(org, project['id'], API_VERSION),
                '$top',
                '$skip'
            ):
                pageNum += 1
                boards = response.json()['value']

                # Convert to ID + JSON, including org and project
                outputItems = list(map(lambda item: translateObject(item, org, project['id']), boards))

                emit_bookmark_records(
                    streamId,
                    schema,
                    org,
                    project,
                    outputItems,
                    extraction_time,
                    counter,
                    mdata,
                    state,
                    '/'.join([streamId, str(pageNum)])
                )
        except InternalServerError as ex:
            ex_str = str(ex)
            if any(ignored_ex in ex_str for ignored_ex in ignored_exceptions):
                logger.warning("Ignoring handled internal server error for boards: %s", ex_str)
            else:
                raise ex
    return state

def sync_all_iterations(schema, org, project, teams, state, mdata, start_date):
    # bookmarks not used for this stream
    streamId = 'iterations'
    logger.info("Syncing all iterations")

    with metrics.record_counter(streamId) as counter:
        extraction_time = singer.utils.now()
        pageNum = 0
        for response in authed_get_all_pages(
            'iterations',
            "https://dev.azure.com/{}/{}/_apis/work/teamsettings/iterations?api-version={}".format(org, project['id'], API_VERSION),
            '$top',
            '$skip'
        ):
            pageNum += 1
            iterations = response.json()['value']

            # Convert to ID + JSON, including org and project
            outputItems = list(map(lambda item: translateObject(item, org, project['id']), iterations))

            emit_bookmark_records(
                streamId,
                schema,
                org,
                project,
                outputItems,
                extraction_time,
                counter,
                mdata,
                state,
                '/'.join([streamId, str(pageNum)])
            )
    return state

def sync_all_work_item_types(schema, org, project, teams, state, mdata, start_date):
    # bookmarks not used for this stream
    streamId = 'workitemtypes'
    logger.info("Syncing all work item types")

    with metrics.record_counter(streamId) as counter:
        extraction_time = singer.utils.now()

        pageNum = 0
        for response in authed_get_all_pages(
            'workitemtypes',
            "https://dev.azure.com/{}/{}/_apis/wit/workitemtypes?api-version={}".format(org, project['id'], API_VERSION),
            '$top',
            '$skip'
        ):
            pageNum += 1
            workitemtypes = response.json()['value']

            # Convert to ID + JSON, including org and project
            outputItems = list(map(lambda item: translateObject(item, org, project['id']), workitemtypes))

            emit_bookmark_records(
                streamId,
                schema,
                org,
                project,
                outputItems,
                extraction_time,
                counter,
                mdata,
                state,
                '/'.join([streamId, str(pageNum)])
            )
    return state

def sync_all_work_item_type_categories(schema, org, project, teams, state, mdata, start_date):
    # bookmarks not used for this stream
    streamId = 'workitemtypecategories'
    logger.info("Syncing all work item type categories")

    with metrics.record_counter(streamId) as counter:
        extraction_time = singer.utils.now()

        pageNum = 0
        for response in authed_get_all_pages(
            'workitemtypecategories',
            "https://dev.azure.com/{}/{}/_apis/wit/workitemtypecategories?api-version={}".format(org, project['id'], API_VERSION),
            '$top',
            '$skip'
        ):
            pageNum += 1
            workitemtypecategories = response.json()['value']

            # Convert to ID + JSON, including org and project
            outputItems = list(map(lambda item: translateObject(item, org, project['id']), workitemtypecategories))

            emit_bookmark_records(
                streamId,
                schema,
                org,
                project,
                outputItems,
                extraction_time,
                counter,
                mdata,
                state,
                '/'.join([streamId, str(pageNum)])
            )

    return state

def sync_all_team_members(schema, org, project, teams, state, mdata, start_date):
    # bookmarks not used for this stream
    streamId = 'teammembers'
    logger.info("Syncing all team members")

    with metrics.record_counter(streamId) as counter:
        extraction_time = singer.utils.now()

        for team in teams:
            teamId = team['id']

            pageNum = 0
            for response in authed_get_all_pages(
                'teammembers',
                "https://dev.azure.com/{}/_apis/projects/{}/teams/{}/members?api-version={}".format(
                    org, project['id'], teamId, API_VERSION),
                '$top',
                '$skip'
            ):
                pageNum += 1
                teammembers = response.json()['value']

                # Convert to ID + JSON, including org and project
                outputItems = list(map(lambda item: translateTeamMember(item, org, project['id'], teamId), teammembers))

                emit_bookmark_records(
                    streamId,
                    schema,
                    org,
                    project,
                    outputItems,
                    extraction_time,
                    counter,
                    mdata,
                    state,
                    '/'.join([streamId, teamId, str(pageNum)])
                )

    return state

def translateTeamMember(item, org, projectId, teamId):
    itemCopy = item['identity'].copy()
    itemCopy['org'] = org
    itemCopy['project'] = projectId
    itemCopy['team'] = teamId
    # Create ID for this relation using ID of team and person
    return {
        'id': '/'.join([teamId, itemCopy['id']]),
        'object': json.dumps(itemCopy),
    }


def sync_all_workitems(schema, org, project, teams, state, mdata, start_date):
    global workItemIds

    streamId = 'workitems'
    bookmarkDate = get_bookmark_date(state, project['id'], streamId, 'since', start_date)
    logger.info("Syncing work items since = %s", bookmarkDate.isoformat())

    with metrics.record_counter(streamId) as counter:
        extraction_time = singer.utils.now()

        itemRefs = None
        from_change_date = bookmarkDate.isoformat()
        maxWiqlResults = 20000 - 1 # at 20000 the API will fail with an error
        while itemRefs is None or len(itemRefs) == maxWiqlResults:
            # WIQL ref: https://learn.microsoft.com/en-us/azure/devops/boards/queries/wiql-syntax?view=azure-devops
            wiql = """
                SELECT [System.Id]
                FROM WorkItems
                WHERE [System.ChangedDate] >= '{}'
                AND [System.TeamProject] = @project
                ORDER BY [System.ChangedDate]
                """.format(from_change_date)
            body = {
                "query": wiql,
            }
            headers = {
                "Content-Type": "application/json",
                "Accept": "application/json",
            }

            logger.info(wiql)
            # first, we need to execute a WIQL query to get a list of all the ticket IDs
            # that have been modified since our last run. despite what it may seem, selecting
            # more fields in the WIQL above will not return them, and instead it always only
            # returns the ticket ID as a shallow ref.
            response = authed_post(
                streamId,
                "https://dev.azure.com/{}/{}/_apis/wit/wiql?api-version={}&$top={}&timePrecision=true".format(org, project['id'], API_VERSION, maxWiqlResults),
                body,
                headers,
            )

            resp_json = response.json()
            itemRefs = resp_json['workItems']
            logger.info('Work items returned by wiql query : {}'.format(len(itemRefs)))

            # now we need to page through the results 200 at a time (max allowed by the API)
            # in order to get the actual ticket field values
            itemRefsCount = len(itemRefs)
            startIndex = 0
            pageSize = 200 # max number of items which can be queried in batch
            while startIndex < itemRefsCount:
                extraction_time = singer.utils.now()
                batchItemRefs = itemRefs[startIndex:(startIndex + pageSize)]
                batchItemIds = list(map(lambda item: item['id'], batchItemRefs))
                body = {
                    "ids": batchItemIds,
                    # Omitting this gets all the fields
                    # "fields": list(dict.keys(FIELD_MAP)),
                }
                startIndex += pageSize
                response = authed_post(
                    'workitemsbatch',
                    "https://dev.azure.com/{}/{}/_apis/wit/workitemsbatch?api-version={}".format(org, project['id'], API_VERSION),
                    body,
                    headers,
                )
                items = response.json()['value']
                # Advance changed date to max of all items
                from_change_date = max(
                    list(
                        map(lambda item: item['fields']['System.ChangedDate'], items)
                    )
                )

                # Convert to ID + JSON, including org and project
                outputItems = list(map(lambda item: translateWorkItem(item, org, project['id']), items))

                emit_records(streamId, schema, org, project, outputItems, extraction_time, counter, mdata)

                # Query and emit update records for each work item

                # add item ids to the global work item IDs for sub-streams to process (e.g. updates)
                for workItemId in batchItemIds:
                    sync_updates_for_work_item(
                        schema,
                        org,
                        project,
                        workItemId,
                        bookmarkDate,
                        mdata,
                    )

                singer.write_bookmark(state, project['id'], streamId, {
                    'since': from_change_date,
                })
                singer.write_state(state)

    return state

def generate_sdc_id(org, id):
    return "{}/{}".format(org, id)

def translateWorkItem(item, org, projectId):
    itemCopy = item.copy()
    itemCopy['org'] = org
    itemCopy['project'] = projectId
    # derive our own ID because unfortunately the ID of a work item is not a UUID
    # and instead is an incrementing integer within an Azure org
    return {
        'id': generate_sdc_id(org, item['id']),
        'object': json.dumps(itemCopy),
    }

def sync_updates_for_work_item(schema, org, project, workItemId, bookmarkDate, mdata):
    streamId = 'updates'
    logger.info("Syncing work item updates since = %s for item %s", bookmarkDate.isoformat(), workItemId)

    with metrics.record_counter(streamId) as counter:
        extraction_time = singer.utils.now()

        # ref: https://learn.microsoft.com/en-us/rest/api/azure/devops/wit/updates/list?view=azure-devops-rest-7.1&tabs=HTTP
        for response in authed_get_all_pages(
            streamId,
            "https://dev.azure.com/{}/{}/_apis/wit/workItems/{}/updates?api-version={}".format(org, project['id'], workItemId, API_VERSION),
            '$top',
            '$skip'
        ):
            updates = response.json()['value']
            updatesToProcess = []

            for update in updates:
                # if this revision was ingested previously, then skip it. there is no way to make the
                # API call itself filter these out
                revisedDate = parse_iso8601(update['revisedDate'])
                if revisedDate < bookmarkDate:
                    continue

                # Convert to ID + JSON, including org and project
                outputUpdate = translateUpdate(update, org, project['id'])
                updatesToProcess.append(outputUpdate)

            emit_records(streamId, schema, org, project, updatesToProcess, extraction_time, counter, mdata)

def translateUpdate(update, org, projectId):
    updateCopy = update.copy()
    updateCopy['org'] = org
    updateCopy['project'] = projectId
    updateCopy['workItemSdcId'] = generate_sdc_id(org, update['workItemId'])
    # derive our own ID because unfortunately the ID of a work item is not a UUID
    # and instead is an incrementing integer within an Azure org
    return {
        'id': generate_sdc_id(org, update['id']),
        'object': json.dumps(updateCopy),
    }

def sync_all_teams(schema, org, project, teams, state, mdata, start_date):
    # bookmarks not used for this stream
    streamId = 'teams'
    logger.info("Syncing all teams")
    with metrics.record_counter(streamId) as counter:
        extraction_time = singer.utils.now()
        # Convert to ID + JSON, including org and project
        outputItems = list(map(lambda item: translateObject(item, org, project['id']), teams))

        emit_bookmark_records(
            streamId,
            schema,
            org,
            project,
            outputItems,
            extraction_time,
            counter,
            mdata,
            state,
            streamId
        )
    return state

def sync_project(schema, org, project, teams, state, mdata, start_date):
    # bookmarks not used for this stream
    streamId = 'projects'
    logger.info("Syncing project %s (ID = %s)", project.get('name'), project.get('id'))
    with metrics.record_counter(streamId) as counter:
        extraction_time = singer.utils.now()
        # Convert to ID + JSON, including org and project
        outputItems = list(map(lambda item: translateObject(item, org, project['id']), [project]))

        emit_bookmark_records(
            streamId,
            schema,
            org,
            project,
            outputItems,
            extraction_time,
            counter,
            mdata,
            state,
            streamId
        )
    return state

def get_selected_streams(catalog):
    '''
    Gets selected streams based on the 'selected' property.
    '''
    selected_streams = []
    for stream in catalog['streams']:
        if stream['schema'].get('selected', False):
            selected_streams.append(stream['tap_stream_id'])

    return selected_streams

def get_stream_from_catalog(stream_id, catalog):
    for stream in catalog['streams']:
        if stream['tap_stream_id'] == stream_id:
            return stream
    return None

SYNC_FUNCTIONS = {
    'boards': sync_all_boards,
    'iterations': sync_all_iterations,
    'projects': sync_project,
    'teams': sync_all_teams,
    'teammembers': sync_all_team_members,
    'workitems': sync_all_workitems,
    'workitemtypes': sync_all_work_item_types,
    'workitemtypecategories': sync_all_work_item_type_categories,
}

SUB_STREAMS = {
    'workitems': ['updates'],
}

projects = None

def do_sync(config, state, catalog):
    '''
    The state structure will be the following:
    {
      "bookmarks": {
        "project-uuid-1": {
          "workitems": {
            "since": "2018-11-14T13:21:20.700360Z"
          },
          "updates": {
            "since": "2018-11-14T13:21:20.700360Z"
          }
        },
        "project-uuid-2": {
          "workitems": {
            "since": "2018-11-14T13:21:20.700360Z"
          },
          "updates": {
            "since": "2018-11-14T13:21:20.700360Z"
          }
        }
      }
    }
    '''

    state = {"bookmarks": {"c2aaafda-e34f-44a7-b9de-b7586d1faa19": {"boards/1": {"hash": "ab77c631705e8ac4e4c4fc9ec996d1ed"}, "teams": {"hash": "585f76e4ec089faa0e12e03783e727b7"}, "teammembers/3f42f905-13c9-4ef5-9bbb-cfe72e227b8c/1": {"hash": "38da6fc0fe7b68e40ae34f550a1d69e2"}, "projects": {"hash": "cc003f6810340e9b553dfe1968248cfd"}, "iterations/1": {"hash": "fc871e6b9de97729a544f1be81a4b278"}, "workitemtypes/1": {"hash": "3502a6b641c11b38de373f17be02a3a3"}, "workitemtypecategories/1": {"hash": "fa4d13bb83a8c8eac935bbc3236e8e4e"}}, "cca2722e-a6a1-4dbe-b396-453c5a1840f8": {"boards/1": {"hash": "46fda7af6dffdcfd1e58b9ecc0bda4e2"}, "teams": {"hash": "882b0ae274479dfba892e102afa9a082"}, "teammembers/5236ffd3-5d43-4f81-8ea2-ba1fa0292d3d/1": {"hash": "0657f36ff8ddae7272b08ae27ed4af4b"}, "projects": {"hash": "e529cc737904833911ad8d09b734095b"}, "iterations/1": {"hash": "a151404b899fe4ca56c789717543f15b"}, "workitemtypes/1": {"hash": "d3e62eb7ef8a0e01dbd63ea775e62639"}, "workitemtypecategories/1": {"hash": "55a5eebb9634da27746fd6f3a114ab6f"}}, "c4f8699b-61b0-4ca6-aa3f-23eb598e1eee": {"workitems": {"since": "2025-01-23T19:14:50.903Z"}, "boards/1": {"hash": "8c3d9b0a69c949c9fbed18b5dddc3e39"}, "teams": {"hash": "b12d4e07d4bdf078895ed867ae431890"}, "teammembers/f7bdcae5-bcc1-428c-b616-c07ed182266d/1": {"hash": "4dd4cbdf781fbdc5b0263c6b730a499f"}, "teammembers/642d593a-7fee-4239-8270-496faf702f31/1": {"hash": "432ece0c962e477c9aee91ab2fba14cb"}, "teammembers/76301f9a-a4c3-499a-a9d2-ba241485b25e/1": {"hash": "3c697ba8ca507c9d5888976dbf1c531c"}, "teammembers/ec8e6f61-2a09-4987-a1d6-4ca1b2b8ba6e/1": {"hash": "2f29f34288e5cab00a701647842bf9b5"}, "teammembers/1ca1afb3-eaaa-48dc-82b5-dbe32fb560ac/1": {"hash": "37a2b7cbe4449107113fb4b8fa8cdaed"}, "teammembers/861cc2e3-9eb3-4ca1-aeef-31cc91d7f8b4/1": {"hash": "1f27584f5e0134ebfe75c3b8fbea6a58"}, "teammembers/2aedd6e3-3125-4d3a-bbb6-1dbcb85d2a8b/1": {"hash": "dcbb6d61b600b07797d37a0ee063f54f"}, "teammembers/c3581c1d-c230-4e88-8706-e68e780f5944/1": {"hash": "3af78c4b0a874fdbcaddcbc3728b448b"}, "teammembers/d6f05a2e-5365-43c5-aa02-08894b4f6b3f/1": {"hash": "5c2717a31090fa359637e796ba9bc171"}, "projects": {"hash": "ec338289b2933691cd083af885d9f493"}, "iterations/1": {"hash": "a168ba5df9d1b04faccaf9ca51e4d00f"}, "workitemtypes/1": {"hash": "f9f3a7c9dbb8490b75f75cbceab16246"}, "workitemtypecategories/1": {"hash": "e67de31a675ed90d3366367c9dd2f921"}}, "86103399-5563-4085-a615-b830a41f2e89": {"boards/1": {"hash": "0009e46d2f0c2bd6c4ad6e446b06acae"}, "teams": {"hash": "7528d6e6b59105e20e9ccb64c0805c27"}, "teammembers/7b612e8f-42d2-4c71-8555-586f4b637a21/1": {"hash": "2520d373c9570d1bf2a5390fd9b4b13f"}, "projects": {"hash": "62a736857c3f5f942a4745c7718385a4"}, "iterations/1": {"hash": "f6ebb420ac077bc0041231722cef9eb1"}, "workitemtypes/1": {"hash": "30d215550203139300377b7258a91f8f"}, "workitemtypecategories/1": {"hash": "dcc497a883642042a0adee9b5d02745d"}}, "4e35c3c4-9502-49d3-a868-15c4cc309000": {"boards/1": {"hash": "5ead7f288034031a8fc6248edee823e2"}, "teams": {"hash": "1b61e69a42c4ee8b7974a4ee4402d7ba"}, "teammembers/69d84fb5-ceaf-48dd-aa7d-66354a3b34e9/1": {"hash": "ecc6373abb2b6ab1e1bdea44bd962c45"}, "teammembers/f7599a85-217a-4ec8-992d-be5a77954cce/1": {"hash": "f1be8c5b786e130526daaf28a8184141"}, "projects": {"hash": "bb4f38144351119e5e84b9172f6d3762"}, "iterations/1": {"hash": "8fdc8aba0b5937d40bac9372b6405e66"}, "workitemtypes/1": {"hash": "aec8a090708002f50f1ee641cad456a8"}, "workitemtypecategories/1": {"hash": "effbbf7e34dd53a0f38da374c382f1f4"}}, "eecd7006-f680-4cf1-8d36-b8cb1de23d93": {"boards/1": {"hash": "817318d4073076e27dd836a55cae9d5b"}, "teams": {"hash": "232275cd4550a75e973878f6b8a0662b"}, "teammembers/74c342ad-d2c2-42a3-a56a-a5b7c1cf6560/1": {"hash": "f90570f651a70b6ab8bb0ec7b5dbff8a"}, "projects": {"hash": "0a8b1c536e6314408f5f1a1f4a6cd763"}, "iterations/1": {"hash": "656462a1cfd0845b8a63d372375f56be"}, "workitemtypes/1": {"hash": "4eb72da9e2244cf70aa11de3bbf3f284"}, "workitemtypecategories/1": {"hash": "fda9dbe85bd31fb081431210c86c3149"}}, "ddbee6e8-748a-4ffc-82ff-9b2b12b3b419": {"boards/1": {"hash": "e88280893ff9b53e33007e9953cb45dd"}, "teams": {"hash": "75ff9fb1895427a75e009618cd1f36a5"}, "teammembers/9c6e0dfb-d2e9-423d-b1a6-25d9cf2acda0/1": {"hash": "286b22c7b566a2ad3a623950d1193855"}, "projects": {"hash": "e627fa0434755e52c13bf3a540cc77c1"}, "iterations/1": {"hash": "30249d5217e6c8e6804f89f95197aa5c"}, "workitemtypes/1": {"hash": "dd77b6788ede2d8ed5f7b93d3abe48dc"}, "workitemtypecategories/1": {"hash": "8f313889741ddd3d6622f0d35bc0ee62"}}, "65f5c566-225c-40a7-8c79-ff418406ad1d": {"teams": {"hash": "0920b4ca2a7012f56f38142b0f70bfa9"}, "teammembers/832792dd-7b1a-48c0-b8e7-4bfb028152cd/1": {"hash": "a1e4d9b37706879f9af3c2eea78b028b"}, "projects": {"hash": "867030e0db0209057bce506bb2340cad"}, "iterations/1": {"hash": "d751713988987e9331980363e24189ce"}, "workitemtypes/1": {"hash": "15a7206723661188aedbbfea5a58e5f0"}, "workitemtypecategories/1": {"hash": "cbfbec8ca8365c218da7bdad3f5775d1"}}, "e8edaaba-f64e-4487-a7a9-4a50fe326661": {"boards/1": {"hash": "4cc7fffe9d8eda89ed9f067552669ec4"}, "teams": {"hash": "3d86466e5f5170ce3e0079680271f091"}, "teammembers/242e9be2-5906-4bcb-8297-de90d5ca84ad/1": {"hash": "e9c2a9dc3ad185baa6b5e63d2d87f782"}, "projects": {"hash": "35913bb9be154830d088b60e13f6ff7e"}, "iterations/1": {"hash": "0ca0bdfe338ca88f9f0240e7d4efd257"}, "workitemtypes/1": {"hash": "88ad26ae01dda77daa90ad5c79054040"}, "workitemtypecategories/1": {"hash": "a156422bf08dfd82001fec16a304471d"}}, "c051b3e9-4330-4afe-b677-567545b7ac0b": {"boards/1": {"hash": "7a4ddcdf3f5b83da746f2c446981413f"}, "teams": {"hash": "808789f76f6e780cd5cc46784bdbdc25"}, "teammembers/d43375ac-8a88-4493-acde-eeb16831b26f/1": {"hash": "b5e1d6bf9a0c81d28cef54d69a66fe1d"}, "projects": {"hash": "d0ac02e8a52d043e03d37437d7194315"}, "iterations/1": {"hash": "ec3e604b987e2792a36e09401ca973e3"}, "workitemtypes/1": {"hash": "fba531a2db68c955d43de420bae63ac1"}, "workitemtypecategories/1": {"hash": "1480564f916543b58b065bd609c41091"}}, "88eff832-c003-4d9c-b521-30531e12a467": {"boards/1": {"hash": "cfe3019a518eded8ba5c5de3f7129a95"}, "teams": {"hash": "298ba0fa5433e78db6740a71bf072ea5"}, "teammembers/10055c6e-5b65-4c55-8c43-9b804c8f0969/1": {"hash": "087642efd7514f84fad6bfc7400e8fe7"}, "projects": {"hash": "c89f929650e67419d9b58e21fe8547d2"}, "iterations/1": {"hash": "e2ffd7b9120619202fdfe6cedb916f30"}, "workitemtypes/1": {"hash": "ee5acfd84b3c94d3cd7549bd31f60dac"}, "workitemtypecategories/1": {"hash": "ca1b6b0fd331fa9a7932cae7b4e9f283"}}, "f16afb34-e357-497f-839a-df8826e2aa3e": {"workitems": {"since": "2025-01-23T19:24:19.243Z"}, "boards/1": {"hash": "5f234369aa35a12a69469bbfd8a843cb"}, "teams": {"hash": "dc87a3bfb3bb8dff8e540927840da1eb"}, "teammembers/e69036af-69ce-42b7-9a09-e4e286ed6673/1": {"hash": "882643af5bd1b501a26acc4c83b691f8"}, "teammembers/14709da0-31fb-4b3d-8b50-39ca103a6f8b/1": {"hash": "006b0698cd0c93b57ea7a95c793375d6"}, "teammembers/c6fed0a8-8743-496b-97de-0b7202a31329/1": {"hash": "356694fae69d479414240cbdeb53d3f3"}, "teammembers/f3d33d46-6eea-4ca0-ae4b-66f7c11d9c1e/1": {"hash": "8172141eff1fe76ee1f2a191441b0a93"}, "teammembers/87c67146-06b2-404f-9271-255fe84fdea8/1": {"hash": "97014bc90b6775021c807fa5b678b0b4"}, "teammembers/ff0467dd-b8cc-458b-b5ea-0b1ab7ab19b8/1": {"hash": "ab8c9486c4ed831c2e2de5fd726b2501"}, "teammembers/aa7068cb-9620-4593-80f4-5709100a5ebc/1": {"hash": "dd0f4d32312831dbbea15c57287f2fa2"}, "teammembers/22a8f515-6a24-44cb-8d3d-da3aaa5fc363/1": {"hash": "e4469f2e635482831877ad6b9d055e36"}, "teammembers/40328d56-ab5c-4ea2-9591-a0cb27372366/1": {"hash": "574a5abb7a05665c976c35b3ad1cebc7"}, "teammembers/ef72bc6c-c6bd-4ab5-bda2-25c773e614cd/1": {"hash": "c7a9d8ddb1839fc636bfdf8482f1457b"}, "teammembers/1d6d10fb-abd6-4ac6-92bb-104397f10f9e/1": {"hash": "f475d421bdccb4af6eb7451e89eee299"}, "teammembers/53d0e7a7-e285-4d95-9a24-fd4e4b456b56/1": {"hash": "0da1afdef8d0b94b50c008e8abb707c0"}, "teammembers/c4dd3a1c-9a3e-4185-97fd-1d6d1975f885/1": {"hash": "d10998c841473b78ad214f0c4c0f007a"}, "teammembers/0dd8c3b2-4064-417e-a2c2-edffa459b94a/1": {"hash": "5de0b211018815b565a97461b0debd7b"}, "teammembers/70aa3e7e-8dec-4091-860a-72729c86f22d/1": {"hash": "a1e85110675a795bd47e7ce2ce8033c0"}, "teammembers/e8ed1f2b-cc5b-4215-b8ce-83097d9810c0/1": {"hash": "9d186377b799ad48dfe967a609151273"}, "teammembers/bb6df1da-f2f2-4464-81c1-f32512167915/1": {"hash": "0848289d559f3f34e915a88e5bcf2351"}, "teammembers/8f2c0db9-f3f6-487d-b045-76eb0c0031ce/1": {"hash": "d4a61cd043b1b739af5cdcbd79e5e060"}, "teammembers/7a034127-b58f-4ad7-9b0b-05c42df3f21a/1": {"hash": "4808f8d474934899162a198889c9a04f"}, "teammembers/2aadd82e-bcf2-4ad6-8ce0-5f0e9adb4280/1": {"hash": "809ba447f5273dbcf684ca15e0bffba6"}, "teammembers/60511386-464f-4807-b29c-17fc94d836ad/1": {"hash": "55bccba5bd4cda59567d185e1b8be390"}, "teammembers/b87455b7-4352-490d-86dd-022937afd7c5/1": {"hash": "0c3795a5a56a5033e3c915f614e26429"}, "projects": {"hash": "c951ef781164ac07358d696bc424df56"}, "iterations/1": {"hash": "d751713988987e9331980363e24189ce"}, "workitemtypes/1": {"hash": "94ca149b3e34d8009b54e699fc4d84dc"}, "workitemtypecategories/1": {"hash": "c4b409e9e62e975da90d1fed163f48d8"}}, "f42f9548-28b4-475f-9924-d37bbe16badf": {"boards/1": {"hash": "fd03b52858a10b8d0460bc3a7c6508b3"}, "teams": {"hash": "891e19054226050bba87f0175a866c40"}, "teammembers/1f7b998a-fe39-49e9-8c80-8d71ac0600cc/1": {"hash": "387d22ad8915623c15eebe18f9a419fc"}, "projects": {"hash": "66c5f85d693a4f15edf915dad9ce5fd9"}, "iterations/1": {"hash": "8a52b5936b35f0be7a4b375d30e52003"}, "workitemtypes/1": {"hash": "686b9c43ecff6e485394158ed7f31031"}, "workitemtypecategories/1": {"hash": "bf5d90d9612d669f565429856823f2fb"}}, "1e11335a-ec47-48b6-96c6-9142a953a6d3": {"teams": {"hash": "c122ae95298a605ad5b603b3a7c4496f"}, "teammembers/ea0e27ff-8ae1-4500-a312-54b78ed5ea4e/1": {"hash": "d751713988987e9331980363e24189ce"}, "projects": {"hash": "8c078de675036bab95740f0bdf52072c"}, "iterations/1": {"hash": "d751713988987e9331980363e24189ce"}, "workitemtypes/1": {"hash": "273c02a1f7145bae50d946d8739ce67d"}, "workitemtypecategories/1": {"hash": "e484019c00728be858f1685e7d2133d4"}}, "f67470e5-a40a-4f69-af08-10c031964f74": {"boards/1": {"hash": "bc319d740101473719f92a61c6d92772"}, "teams": {"hash": "58b44245edc6c87da31f8399d2044b07"}, "teammembers/469e248c-b666-4de1-aa97-6345c3c0ceb9/1": {"hash": "ff3b7fc7ee6209ea12f4daa5bbe120cf"}, "projects": {"hash": "c4b236107888a0d830263a3b0e965ff8"}, "iterations/1": {"hash": "90bc97ff05f11c58025b2b63f8aa4c40"}, "workitemtypes/1": {"hash": "83bde8390856ee4c1dbde5c172427e2c"}, "workitemtypecategories/1": {"hash": "9d379b775c0cfbdc04f6932d85059d4b"}}, "4b901f77-8c14-48a5-96ca-6c1670ddd7b5": {"teams": {"hash": "875bcd62a450fbeadbdc32b785ab8ca8"}, "teammembers/a70f23e2-6c1f-4296-b6a4-fd3fdd9d88ea/1": {"hash": "d751713988987e9331980363e24189ce"}, "projects": {"hash": "f7b1d6dd89c759f302c91901c23be93d"}, "iterations/1": {"hash": "d751713988987e9331980363e24189ce"}, "workitemtypes/1": {"hash": "809989a36b17f8b47bad769a23e1497a"}, "workitemtypecategories/1": {"hash": "93b7a18a3f3736000f63c798e238e73e"}}, "67563a49-e5fc-47d2-8a8d-46538d7964bd": {"boards/1": {"hash": "e08ffbc930283321f9278bfb0f4b73cf"}, "teams": {"hash": "2ae8bdceb9dccf46689c2fcedce615fc"}, "teammembers/0713246f-12a4-4c13-9f6c-c8896cb3685a/1": {"hash": "eed51a19fceae7b9c7a18aa8d3920907"}, "teammembers/3021c4e6-06b6-42bb-aafd-0ce8db6ff6f7/1": {"hash": "f83f8f2bd5eb0d84487de43afce97130"}, "projects": {"hash": "fff1ae01b2e82fca64e9b69053ecd0db"}, "iterations/1": {"hash": "c6aacc893c1625b6785c97ddd2470377"}, "workitemtypes/1": {"hash": "723f76a002ed3f0ad29ebc9aa741aff7"}, "workitemtypecategories/1": {"hash": "4355f02faf5636dcdd641e385ec478e0"}}, "df67b058-1ed3-41f0-ae21-15187792cf84": {"workitems": {"since": "2025-01-23T13:59:06.667Z"}, "boards/1": {"hash": "601a432bac5effcae012dbcce878a1a4"}, "teams": {"hash": "4ff5b0017f8a709cafffa2dbe83b18d3"}, "teammembers/56bfff0c-e344-4002-a33c-e183364815f8/1": {"hash": "e84a3131e7f2ac5b0ce7e1615d4edbbd"}, "teammembers/3f5adb00-0708-49ad-aed4-44185cd7eec7/1": {"hash": "06fc4edcaaee2474765e9ee68d1781fa"}, "projects": {"hash": "654c87ce23737cba397fad9b75426b37"}, "iterations/1": {"hash": "593ca6b80a29a72f5d50d0e9c9ecde30"}, "workitemtypes/1": {"hash": "a8d6f9f59da5f507d4482a57f694e243"}, "workitemtypecategories/1": {"hash": "49305c73a6dd156c382e1e0e7f0a55aa"}}, "02b5971e-3831-4e8e-86cd-3cbdbdb4654a": {"teams": {"hash": "707b016fa0615d4dea5e71a0ed7ebd49"}, "teammembers/10b72788-3e4f-4aad-bbb4-8c9e97dba82c/1": {"hash": "d751713988987e9331980363e24189ce"}, "projects": {"hash": "3deeb28ffca94a5a89fd830723d04642"}, "iterations/1": {"hash": "d751713988987e9331980363e24189ce"}, "workitemtypes/1": {"hash": "e224a3a6b8a1306f661dc00d2603c87b"}, "workitemtypecategories/1": {"hash": "b926039e7e4b4a8223a10c10ce0b355a"}}, "62e30733-cac6-4141-8ff8-c8947838f000": {"boards/1": {"hash": "46ccd13d3ae0a0f2f61bfa215fda3b10"}, "teams": {"hash": "6c6901aa0d47bbceaf69ebafafcee8e6"}, "teammembers/26153f4b-bfd1-4cd1-a70d-29c5a751c592/1": {"hash": "19d16a1b2eef887c60c1e7c1d5458f50"}, "projects": {"hash": "6afec0b88e38f7ffa4211f75c794bbc6"}, "iterations/1": {"hash": "d751713988987e9331980363e24189ce"}, "workitemtypes/1": {"hash": "b497f52269dc5fd7abeeaeec143a0957"}, "workitemtypecategories/1": {"hash": "c8a9138756393b5b9d496362b257f08f"}}, "090fb689-cacf-4e0a-b4f6-bdbbdf022ac5": {"boards/1": {"hash": "872babfca8b6df5e440e28d2a93b015c"}, "teams": {"hash": "ee37b32268ce4e71ce98048677f05ae9"}, "teammembers/052b9706-2b99-4cc2-97b0-5f232b3f3ae0/1": {"hash": "4dde511aab45d5f5c6a3a847babcb1e7"}, "projects": {"hash": "c75e9019b9b8194a5b246f2f14d3c94b"}, "iterations/1": {"hash": "3e043e8e2d7412432d88948198da82ff"}, "workitemtypes/1": {"hash": "d0e39c0908997dca17f0bd1bafd8ee02"}, "workitemtypecategories/1": {"hash": "76a838896cacbfe78f554806a964ca55"}}, "1e4fa473-cda0-4728-87a3-b7957c12ab24": {"teams": {"hash": "7bd70e1f55aa2fba64165bdcca9584b7"}, "teammembers/44fbea99-77bd-4ca7-8baa-879b056eb065/1": {"hash": "acc90ab1eef6c1ee0660d2570d417df8"}, "projects": {"hash": "3519490581bed476e994e51932e514c4"}, "iterations/1": {"hash": "d751713988987e9331980363e24189ce"}, "workitemtypes/1": {"hash": "57424ae97228b374b0424c26b04e811d"}, "workitemtypecategories/1": {"hash": "6eb1f0d631d592d9c1db063e2aaa0b10"}}, "eb994411-e356-4379-9f66-f7ed0222a048": {"workitems": {"since": "2025-01-23T16:11:53.23Z"}}}}

    start_date = config['start_date'] if 'start_date' in config else None

    # get selected streams, make sure stream dependencies are met
    selected_stream_ids = get_selected_streams(catalog)
    validate_dependencies(selected_stream_ids)

    # sort streams such that sub streams always come after their parent stream dependency
    stream_sorter = lambda a, b: -1 if SUB_STREAMS.get(a['tap_stream_id']) else 1
    catalog['streams'].sort(key=functools.cmp_to_key(stream_sorter))

    org = config['org']

    # load all the projects which match the filter provided by our config
    projects = get_selected_projects(config['org'], config['projects'])

    # logger.info("Projects: %s", projects)

    # for each project, run each selected steam procesor
    is_schema_written = {}
    for project in projects:
        logger.info("Starting sync of project: %s using start date: %s", project['name'], start_date)

        teams = get_teams_for_project(org, project['id'])

        for stream in catalog['streams']:
            stream_id = stream['tap_stream_id']
            stream_schema = stream['schema']
            mdata = stream['metadata']

            # if it is a "sub_stream", it will be sync'd by its parent (just updates for now)
            if not SYNC_FUNCTIONS.get(stream_id):
                continue

            # if stream is selected, write schema (once only) and sync
            if stream_id in selected_stream_ids:
                if is_schema_written.get(stream_id) is None:
                    singer.write_schema(stream_id, stream_schema, stream['key_properties'])
                    is_schema_written[stream_id] = True

                sync_func = SYNC_FUNCTIONS[stream_id]
                state = sync_func(stream_schema, org, project, teams, state, mdata, start_date)

    singer.write_state(state)

@singer.utils.handle_top_exception(logger)
def main():
    args = singer.utils.parse_args(REQUIRED_CONFIG_KEYS)

    if args.discover:
        do_discover(args.config)
    else:
        # Initialize basic auth
        user_name = args.config['user_name']
        access_token = args.config['access_token']
        session.auth = (user_name, access_token)

        catalog = args.properties if args.properties else get_catalog()
        do_sync(args.config, args.state, catalog)

if __name__ == '__main__':
    main()
