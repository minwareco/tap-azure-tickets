import os
import json
import functools
from dateutil import parser
import pytz
import time
import requests
import re
import singer
import singer.bookmarks as bookmarks
import singer.metrics as metrics


from singer import metadata

session = requests.Session()
logger = singer.get_logger()

# globals which hold data across streams
workItemIds = set()

# map of Azure field names to more usable/readable field names
FIELD_MAP = {
    "System.Id": "id",
    "System.AssignedTo": "assignedTo",
    "Microsoft.VSTS.Common.ClosedBy": "closedBy",
    "Microsoft.VSTS.Common.ClosedDate": "closedDate",
    "Microsoft.VSTS.CodeReview.ClosedStatus": "closedStatus",
    "Microsoft.VSTS.CodeReview.ClosedStatusCode": "closedStatusCode",
    "System.ChangedDate": "changedDate",
    "System.ChangedBy": "changedBy",
    "System.CreatedBy": "createdBy",
    "System.CreatedDate": "createdDate",
    "System.Description": "description",
    "Microsoft.VSTS.Scheduling.Effort": "effort",
    "Microsoft.VSTS.Scheduling.FinishDate": "finishDate",
    "Microsoft.VSTS.Common.Priority": "priority",
    "Microsoft.VSTS.Common.ResolvedBy": "resolvedBy",
    "Microsoft.VSTS.Common.ResolvedDate": "resolvedDate",
    "Microsoft.VSTS.Common.StackRank": "stackRank",
    "Microsoft.VSTS.Scheduling.StartDate": "startDate",
    "System.State": "state",
    "Microsoft.VSTS.Common.StateChangeDate": "stateChangeDate",
    "System.IterationPath": "iterationPath",
    "System.Title": "title",
    "System.WorkItemType": "type",
}

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
    'updates': ['_sdc_id'],
    'workitems': ['_sdc_id'],
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

    exc = ERROR_CODE_EXCEPTION_MAPPING.get(error_code, {}).get("raise_exception", AzureException)
    raise exc(message) from None

def calculate_seconds(epoch):
    current = time.time()
    return int(round((epoch - current), 0))

def rate_throttling(response):
    '''
    See documentation here, which recommends at least sleeping if a Retry-After header is sent.

    https://docs.microsoft.com/en-us/azure/devops/integrate/concepts/rate-limits?view=azure-devops
    '''
    if 'Retry-After' in response.headers:
        waitTime = int(response.headers['Retry-After'])
        if waitTime < 1:
            # Probably should never happen, but sleep at least one second if this header for some
            # reason isn't a valid int or is less than 1
            waitTime = 1
        logger.info("API Retry-After wait time header found, sleeping for {} seconds." \
            .format(waitTime))
        time.sleep(waitTime)

# pylint: disable=dangerous-default-value
def authed_get(source, url, headers={}):
    with metrics.http_request_timer(source) as timer:
        session.headers.update(headers)
        # Uncomment for debugging
        #logger.info("requesting {}".format(url))
        resp = session.request(method='get', url=url)

        if resp.status_code != 200:
            raise_for_error(resp, source, url)
        timer.tags[metrics.Tag.http_status_code] = resp.status_code
        rate_throttling(resp)
        return resp

# pylint: disable=dangerous-default-value
def authed_post(source, url, json, headers={}):
    with metrics.http_request_timer(source) as timer:
        session.headers.update(headers)
        # Uncomment for debugging
        #logger.info("requesting {}".format(url))
        resp = session.request(method='post', url=url, json=json)

        if resp.status_code != 200:
            raise_for_error(resp, source, url)
        timer.tags[metrics.Tag.http_status_code] = resp.status_code
        rate_throttling(resp)
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

def sync_all_boards(schema, org, project, teams, state, mdata, start_date):
    # bookmarks not used for this stream
    streamId = 'boards'
    logger.info("Syncing all boards")

    with metrics.record_counter(streamId) as counter:
        extraction_time = singer.utils.now()
        for response in authed_get_all_pages(
            'boards',
            "https://dev.azure.com/{}/{}/_apis/work/boards?api-version={}".format(org, project['id'], API_VERSION),
            '$top',
            '$skip'
        ):
            boards = response.json()['value']
            emit_records(streamId, schema, org, project, boards, extraction_time, counter, mdata)
    return state

def sync_all_iterations(schema, org, project, teams, state, mdata, start_date):
    # bookmarks not used for this stream
    streamId = 'iterations'
    logger.info("Syncing all iterations")

    with metrics.record_counter(streamId) as counter:
        extraction_time = singer.utils.now()
        for response in authed_get_all_pages(
            'boards',
            "https://dev.azure.com/{}/{}/_apis/work/teamsettings/iterations?api-version={}".format(org, project['id'], API_VERSION),
            '$top',
            '$skip'
        ):
            iterations = response.json()['value']

            # flatten out some properties for convenience
            for iteration in iterations:
                attributes = iteration.get('attributes') or {}
                iteration['startDate'] = attributes.get('startDate')
                iteration['finishDate'] = attributes.get('finishDate')
                iteration['timeFrame'] = attributes.get('timeFrame')

            emit_records(streamId, schema, org, project, iterations, extraction_time, counter, mdata)
    return state

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
                    "fields": list(dict.keys(FIELD_MAP)),
                }
                startIndex += pageSize
                response = authed_post(
                    'workitemsbatch',
                    "https://dev.azure.com/{}/{}/_apis/wit/workitemsbatch?api-version={}".format(org, project['id'], API_VERSION),
                    body,
                    headers,
                )
                items = response.json()['value']
                normalizedItems = list(map(lambda item: normalize_work_item(item, org, FIELD_MAP), items))
                from_change_date = max(
                    list(
                        map(lambda item: item['changedDate'], normalizedItems)
                    )
                )

                # add item ids to the global work item IDs for sub-streams to process (e.g. updates)
                workItemIds.update(batchItemIds)

                emit_records(streamId, schema, org, project, normalizedItems, extraction_time, counter, mdata)

    singer.write_bookmark(state, project['id'], streamId, {
        'since': singer.utils.strftime(extraction_time),
    })

    return state

def generate_sdc_id(org, id):
    return "{}/{}".format(org, id)

def normalize_work_item(item, org, fieldMap):
    # derive our own ID because unfortunately the ID of a work item is not a UUID
    # and instead is an incrementing integer within an Azure org
    normalizedItem = {
        "_sdc_id": generate_sdc_id(org, item['id'])
    }

    # for each field in the raw work item from Azure, use its mapped name to set the field
    # value into the normalized item
    for fieldName, fieldValue in item['fields'].items():
        normalizedFieldName = fieldMap[fieldName]
        normalizedItem[normalizedFieldName] = fieldValue

    return normalizedItem

def sync_all_updates(schema, org, project, teams, state, mdata, start_date):
    global workItemIds

    streamId = 'updates'
    bookmarkDate = get_bookmark_date(state, project['id'], streamId, 'since', start_date)
    logger.info("Syncing work item updates since = %s for %d items", bookmarkDate.isoformat(), len(workItemIds))

    with metrics.record_counter(streamId) as counter:
        extraction_time = singer.utils.now()

        # iterate over all the work items that were processed so we can absorb their updates
        for workItemId in workItemIds:

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

                    # convert the dict of <string, WorkItemFieldUpdate> to an array of objects suitable
                    # for our desired output schema, which is an array of update objects.
                    # WorkItemFieldUpdate: https://learn.microsoft.com/en-us/rest/api/azure/devops/wit/updates/list?view=azure-devops-rest-7.1&tabs=HTTP#workitemfieldupdate
                    normalizedFields = []
                    # If the only change was adding / removing a relation
                    # no fields are changed
                    if 'fields' in update:
                        for fieldName, values in update['fields'].items():
                            fieldName = FIELD_MAP.get(fieldName)
                            if fieldName is not None:
                                normalizedFields.append({
                                    "fieldName": fieldName,
                                    "oldValue": normalize_value_to_string(values.get('oldValue')),
                                    "newValue": normalize_value_to_string(values.get('newValue')),
                                })

                    # derive our own ID because unfortunately the ID of an update is not a UUID
                    # and instead is an incrementing integer within an Azure org
                    update['_sdc_id'] = generate_sdc_id(org, update['id'])
                    update['workItemSdcId'] = generate_sdc_id(org, update['workItemId'])
                    update['fields'] = normalizedFields
                    updatesToProcess.append(update)

                emit_records(streamId, schema, org, project, updatesToProcess, extraction_time, counter, mdata)

    singer.write_bookmark(state, project['id'], streamId, {
        'since': singer.utils.strftime(extraction_time),
    })

    return state

def normalize_value_to_string(value):
    if value is None:
        return value
    if isinstance(value, str):
        return value
    elif isinstance(value, dict):
        return json.dumps(value)
    else:
        return str(value) # probably a number or boolean

def sync_all_teams(schema, org, project, teams, state, mdata, start_date):
    # bookmarks not used for this stream
    streamId = 'teams'
    logger.info("Syncing all teams")
    with metrics.record_counter(streamId) as counter:
        extraction_time = singer.utils.now()
        emit_records(streamId, schema, org, project, teams, extraction_time, counter, mdata)
    return state

def sync_project(schema, org, project, teams, state, mdata, start_date):
    # bookmarks not used for this stream
    streamId = 'projects'
    logger.info("Syncing project %s (ID = %s)", project.get('name'), project.get('id'))
    with metrics.record_counter(streamId) as counter:
        extraction_time = singer.utils.now()
        emit_records(streamId, schema, org, project, [project], extraction_time, counter, mdata)
    return state

def emit_records(streamId, schema, org, project, objects, extraction_time, counter, mdata):
    mdataMap = metadata.to_map(mdata)
    for object in objects:
        object['org'] = org
        object['project'] = project['id']
        with singer.Transformer() as transformer:
            rec = transformer.transform(object, schema, metadata=mdataMap)
        singer.write_record(streamId, rec, time_extracted=extraction_time)
        if counter:
            counter.increment()


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
    'workitems': sync_all_workitems,
    'updates': sync_all_updates,
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

    # for each project, run each selected steam procesor
    is_schema_written = {}
    for project in projects:
        logger.info("Starting sync of project: %s", project['name'])

        teams = get_teams_for_project(org, project['id'])

        for stream in catalog['streams']:
            stream_id = stream['tap_stream_id']
            stream_schema = stream['schema']
            mdata = stream['metadata']

            # if it is a "sub_stream", it will be sync'd by its parent
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
