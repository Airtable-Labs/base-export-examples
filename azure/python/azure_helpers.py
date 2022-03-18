def add_record_ids(table_data):

    revised_data = []

    for record in table_data:

        fields = record['fields']
        airtable_id = record['id']
        fields['airtable_id'] = airtable_id
        revised_data.append(fields)

    return revised_data
