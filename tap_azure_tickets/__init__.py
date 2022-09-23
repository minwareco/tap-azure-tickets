import argparse
import os
import json
import collections
import time
from dateutil import parser
import pytz
import requests
import re
import psutil
import asyncio
import gc
import singer
import singer.bookmarks as bookmarks
import singer.metrics as metrics
import difflib


from singer import metadata

session = requests.Session()
logger = singer.get_logger()

REQUIRED_CONFIG_KEYS = ['start_date', 'user_name', 'access_token', 'org', 'projects']

KEY_PROPERTIES = {
    'boards': ['id'],
    'projects': ['id'],
}

API_VESION = "6.0"

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

def get_bookmark(state, repo, stream_name, bookmark_key, default_value=None):
    repo_stream_dict = bookmarks.get_bookmark(state, repo, stream_name)
    if repo_stream_dict:
        return repo_stream_dict.get(bookmark_key)
    if default_value:
        return default_value
    return None

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

def get_selected_projects(org, filter):
    for response in authed_get_all_pages(
        'projects',
        "https://dev.azure.com/{}/_apis/boards?api-version={}".format(org, API_VESION),
        '$top',
        'continuationToken'
    ):
        projects = response.json()['value']
        for project in projects:
            logger.info("Loaded project: %s", project)
    
def sync_all_boards(schema, org, project, state, mdata):
    # bookmarks not used for this stream
    
    with metrics.record_counter('boards') as counter:
        extraction_time = singer.utils.now()

        for response in authed_get_all_pages(
            'projects',
            "https://dev.azure.com/{}/_apis/boards?" \
            "api-version={}" \
            .format(org, API_VESION),
            '$top',
            '$skip',
            True # No link header to indicate availability of more data
        ):
            projects = response.json()['value']
            for project in projects:
                projectName = project['name']
                for response in authed_get_all_pages(
                    'repositories',
                    "https://dev.azure.com/{}/{}/_apis/git/repositories?" \
                    "api-version={}" \
                    .format(org, projectName, API_VESION),
                    '$top',
                    '$skip',
                    True # No link header to indicate availability of more data
                ):
                    repos = response.json()['value']
                    for repo in repos:
                        repoName = repo['name']
                        repo['_sdc_repository'] = '{}/{}/_git/{}'.format(org, projectName, repoName)
                        
                        with singer.Transformer() as transformer:
                            rec = transformer.transform(repo, schema,
                                metadata=metadata.to_map(mdata))
                        singer.write_record('repositories', rec, time_extracted=extraction_time)
                        counter.increment()
    return state

def get_selected_streams(catalog):
    '''
    Gets selected streams.  Checks schema's 'selected'
    first -- and then checks metadata, looking for an empty
    breadcrumb and mdata with a 'selected' entry
    '''
    selected_streams = []
    for stream in catalog['streams']:
        stream_metadata = stream['metadata']
        if stream['schema'].get('selected', False):
            selected_streams.append(stream['tap_stream_id'])
        else:
            for entry in stream_metadata:
                # stream metadata will have empty breadcrumb
                if not entry['breadcrumb'] and entry['metadata'].get('selected',None):
                    selected_streams.append(stream['tap_stream_id'])

    return selected_streams

def get_stream_from_catalog(stream_id, catalog):
    for stream in catalog['streams']:
        if stream['tap_stream_id'] == stream_id:
            return stream
    return None

SYNC_FUNCTIONS = {
    'boards': sync_all_boards,
    # 'projects': sync_all_projects,
}

SUB_STREAMS = {
    # 'pull_requests': ['pull_request_threads'],
    # 'commit_files': ['refs']
}

projects = None

def do_sync(config, state, catalog):
    '''
    The state structure will be the following:
    {
      "bookmarks": {
        "project1/repo1": {
          "commits": {
            "since": "2018-11-14T13:21:20.700360Z"
          }
        },
        "project2/repo2": {
          "commits": {
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

    org = config['org']

    # load all the projects which match the filter provided by our config. the projects
    # are cached globally so we can reference them as needed throughout all downstream tap work.
    global projects
    projects = get_selected_projects(config['projects'])
    return

    #pylint: disable=too-many-nested-blocks
    for project in projects:
        logger.info("Starting sync of project: %s", project)
        for stream in catalog['streams']:
            stream_id = stream['tap_stream_id']
            stream_schema = stream['schema']
            mdata = stream['metadata']

            # if it is a "sub_stream", it will be sync'd by its parent
            if not SYNC_FUNCTIONS.get(stream_id):
                continue

            # if stream is selected, write schema and sync
            if stream_id in selected_stream_ids:
                singer.write_schema(stream_id, stream_schema, stream['key_properties'])

                sync_func = SYNC_FUNCTIONS[stream_id]
                state = sync_func(stream_schema, org, project, state, mdata, start_date)

    singer.write_state(state)

@singer.utils.handle_top_exception(logger)
def main():
    args = singer.utils.parse_args(REQUIRED_CONFIG_KEYS)

    # Initialize basic auth
    user_name = args.config['user_name']
    access_token = args.config['access_token']
    session.auth = (user_name, access_token)

    if args.discover:
        do_discover(args.config)
    else:
        catalog = args.properties if args.properties else get_catalog()
        do_sync(args.config, args.state, catalog)

if __name__ == '__main__':
    main()
