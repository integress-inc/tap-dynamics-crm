from datetime import datetime

import singer
from singer import metrics, metadata, Transformer
from singer.bookmarks import set_currently_syncing

from tap_dynamics.discover import discover, get_optionset_metadata, get_optionset_fieldname

LOGGER = singer.get_logger()

MODIFIED_DATE_FIELD = 'modifiedon'
CREATEDON_DATE_FIELD = 'createdon'

def get_bookmark(state, stream_name, default):
    LOGGER.info('')
    LOGGER.info('')
    LOGGER.info('*** Entered get_bookmark ***')
    LOGGER.info('State: "{}"'.format(state))
    LOGGER.info('Stream_Name: "{}", Default: "{}", MODIFIED_DATE_FIELD: "{}"'.format( stream_name, default, MODIFIED_DATE_FIELD))

    output = state.get('bookmarks', {}).get(stream_name, default)

    LOGGER.info('state.get() output: "{}"'.format(output))
    LOGGER.info('*** Leaving get_bookmark and returning above output ***')
    LOGGER.info('')

    return output

def write_bookmark(state, stream_name, value):
    if 'bookmarks' not in state:
        state['bookmarks'] = {}
    state['bookmarks'][stream_name] = value
    singer.write_state(state)

def write_schema(stream):
    schema = stream.schema.to_dict()
    singer.write_schema(stream.tap_stream_id, schema, stream.key_properties)

def sync_stream(service, catalog, state, start_date, stream, mdata):
    stream_name = stream.tap_stream_id
    last_datetime = get_bookmark(state, stream_name, start_date)

    write_schema(stream)

    max_modified = last_datetime

    ## TODO: add metrics?
    entitycls = service.entities[stream_name]
    query = service.query(entitycls)

    if hasattr(entitycls, MODIFIED_DATE_FIELD) or hasattr(entitycls, CREATEDON_DATE_FIELD):
        if hasattr(entitycls, MODIFIED_DATE_FIELD):
            query_field = MODIFIED_DATE_FIELD
        else:
            query_field = CREATEDON_DATE_FIELD # Added to handle the append-only "Audits" table
        LOGGER.info('')
        LOGGER.info('{} - Syncing data since {} using replication_key {}'.format(stream.tap_stream_id, last_datetime, query_field))
        LOGGER.info('')
        query = (
            query
            .filter(getattr(entitycls, query_field) >= singer.utils.strptime_with_tz(last_datetime))
            .order_by(getattr(entitycls, query_field).asc())
        )
    else:
        LOGGER.info('')
        LOGGER.info('{} - Syncing using full replication'.format(stream.tap_stream_id))
        LOGGER.info('')

    schema = stream.schema.to_dict()
    optionset_map = get_optionset_metadata(service, stream.tap_stream_id)
    with metrics.record_counter(stream.tap_stream_id) as counter:
        for record in query:
            dict_record = {}
            for odata_prop in entitycls.__odata_schema__['properties']:
                prop_name = odata_prop['name']
                value = getattr(record, prop_name)
                if isinstance(value, datetime):
                    value = singer.utils.strftime(value)
                dict_record[prop_name] = value

                if prop_name in optionset_map:
                    label_prop_name = get_optionset_fieldname(prop_name)
                    if value is None:
                        #dict_record[label_prop_name] = None
                        # the above turns the Python null of None to the string "None", which is not good for the target DB
                        dict_record[label_prop_name] = ''
                    else:
                        optionset_value = optionset_map[prop_name].get(value)
                        if optionset_value is None:
                            LOGGER.warn('Label not found for value `{}` on `{}`'.format(value, prop_name))
                        dict_record[label_prop_name] = optionset_value

            if MODIFIED_DATE_FIELD in dict_record and dict_record[MODIFIED_DATE_FIELD] > max_modified:
                    max_modified = dict_record[MODIFIED_DATE_FIELD]

            with Transformer() as transformer:
                dict_record_t = transformer.transform(dict_record,
                                                      schema,
                                                      mdata)
            singer.write_record(stream.tap_stream_id, dict_record_t)
            counter.increment()

    write_bookmark(state, stream_name, max_modified)

def update_current_stream(state, stream_name=None):  
    set_currently_syncing(state, stream_name) 
    singer.write_state(state)

def sync(service, catalog, state, start_date):
    if not catalog:
        catalog = discover(service)
        selected_streams = catalog.streams
    else:
        selected_streams = catalog.get_selected_streams(state)

    for stream in selected_streams:
        mdata = metadata.to_map(stream.metadata)
        update_current_stream(state, stream.tap_stream_id)
        sync_stream(service, catalog, state, start_date, stream, mdata)

    update_current_stream(state)
