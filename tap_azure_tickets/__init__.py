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
                    # This is necessary to get the relations and links
                    "$expand": "all"
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

    # Just write out updates schema right away since it is a sub-stream
    updatesSchema = None
    updatesKeyProperties = None
    for stream in catalog['streams']:
        stream_id = stream['tap_stream_id']
        stream_schema = stream['schema']
        if stream_id == 'updates':
            updatesSchema = stream_schema
            updatesKeyProperties = stream['key_properties']
            break
    if updatesSchema:
        singer.write_schema('updates', updatesSchema, updatesKeyProperties)
        is_schema_written['updates'] = True

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
