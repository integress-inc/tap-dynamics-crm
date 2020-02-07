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

    query_field = None

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
                        dict_record[label_prop_name] = None
                    else:
                        optionset_value = optionset_map[prop_name].get(value)
                        if optionset_value is None:
                            LOGGER.warn('Label not found for value `{}` on `{}`'.format(value, prop_name))
                        dict_record[label_prop_name] = optionset_value

            if query_field in dict_record and dict_record[query_field] > max_modified:
                    max_modified = dict_record[query_field]

            # ################################################################################################################################################################
            # This code block is a brute-force fix to elements withing "XX_value" fields coming back as the string "None" instead of "null"
            # when the associated "XX_Code" value is "null".  
            #
            # The pattern can be seen in this raw data extract: 'crm_meetingroomcode': None, 'crm_meetingroomcode_label': None, '_crm_consultantaccountid_value': 'None'
            #
            #    - crm_meetingroomcode              = None, which equates to crm_meetingroomcode = null (None is what's used for null in the Pythong language)
            #    - crm_meetingroomcode_label        = null, which is correct.  This value is looked up in the code above and handled properly.
            #    - _crm_consultantaccountid_value   = 'None', which is a string and INCORRECT - the proper value would be:
            #           - _crm_consultantaccountid_value = null
            #
            # Testing confirmed that this Dynamics Tap is recieving these "XX_value = 'None'" entries, as the dynamics tap does not perform any lookup logic for "XX_value" 
            # columns (only for "XX_Label" columns).  This means the issue is being is being created upstream from they Dynamics Tap - either in the underlying python-odata
            # library or from Dynamics API itself.
            # ################################################################################################################################################################
            # LOGGER.info('')
            # LOGGER.info('')
            # LOGGER.info('***********************************')
            # LOGGER.info('*** BEGIN dict_record item loop ***')
            # LOGGER.info('***********************************')
            for prop, value in dict_record.items():
                # LOGGER.info('')
                # LOGGER.info('Prop: "{}"'.format(prop))
                # LOGGER.info('Value: "{}"'.format(value))

                if value == 'None' and prop[0] == '_' and prop[-6:].lower() == '_value':
                    # LOGGER.info('')
                    # LOGGER.info('*** IF CONDITION TRUE  (if value == "None" and prop[0] == "_" and prop[-6:].lower() == "_value") ***')
                    # LOGGER.info('Before dict_record[prop]: "{}", Type: "{}"'.format(dict_record[prop], type(dict_record[prop])))
                    dict_record[prop] = None
                    # LOGGER.info('After dict_record[prop]: "{}", Type: "{}"'.format(dict_record[prop], type(dict_record[prop])))
            
            # LOGGER.info('***********************************')
            # LOGGER.info('*** END dict_record item loop ***')
            # LOGGER.info('***********************************')
                    
            # Output of this code on 2/6/2020 confirms that works as expected (notice the type change):
            #
            # INFO *** IF CONDITION TRUE  (if value == "None" and prop[0] == "_" and prop[-6:].lower() == "_value") ***
            # INFO Before dict_record[prop]: "None", Type: "<class 'str'>"
            # INFO After dict_record[prop]: "None", Type: "<class 'NoneType'>"
            #
            # And the resultant stream outpout RECORDS shows that "null" is now being used (see the last variable change below):
            # BEFORE:
            #       "crm_caterercode": null, "crm_caterercode_label": null, "_crm_roomcoordinatoruserid_value": "None", 
            # AFTER:
            #       "crm_caterercode": null, "crm_caterercode_label": null, "_crm_roomcoordinatoruserid_value": null,
            ## ################################################################################################################################################################

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
