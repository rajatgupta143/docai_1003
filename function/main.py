from google.cloud import documentai_v1 as documentai,bigquery
import simplejson as json
import proto
from google.cloud import storage
import os
import re

def start1003Parser(event, context):
    project_id = os.environ['project_id']
    processor_id = os.environ['processor_id']
    location = os.environ['location']
    table_id = os.environ['BQ_Table_Id']
    file_path = event['name']
    input_bucket = event['bucket']

    print("project_id:{},processor_id:{},file_path:{}, input bucket:{}".format(project_id,processor_id,file_path,input_bucket))
    
    # You must set the api_endpoint if you use a location other than 'us', e.g.:
    opts = {}
    if location == "eu":
        opts = {"api_endpoint": "eu-documentai.googleapis.com"}

    client = documentai.DocumentProcessorServiceClient(client_options=opts)

    # The full resource name of the processor, e.g.:
    # projects/project-id/locations/location/processor/processor-id
    # You must create new processors in the Cloud Console first
    name = f"projects/{project_id}/locations/{location}/processors/{processor_id}"
        
    image_content = download_blob(input_bucket, file_path).download_as_bytes()
    # Read the file into memory
    document = {"content": image_content, "mime_type": "application/pdf"}

    # Configure the process request
    request = {"name": name, "raw_document": document}

    # Recognizes text entities in the PDF document
    result = client.process_document(request=request)

    document = result.document

    print("Document processing complete.")

    # For a full list of Document object attributes, please reference this page: https://googleapis.dev/python/documentai/latest/_modules/google/cloud/documentai_v1beta3/types/document.html#Document

    entityDict={}
    entityDict["fileName"]=file_path 
    for entity in document.entities:
        entity_type = entity.type_
        if(entity.normalized_value.text!=""):
            entity_text = entity.normalized_value.text
        else:
            entity_text = re.sub('[":\""]', '',entity.mention_text)
        
        # Placeholder code below to test whether the amount fields have strings with commas coming in. Converting them to floats for now.        
        if("amount" in entity_type and entity.normalized_value.text ==''):
            entity_text = float(re.sub('\D', '', entity.mention_text))
        
        entityDict[entity_type]=entity_text 

    #Calling the WiteToBQ Method
    writeToBQ(entityDict, table_id) 
    
# Write to BQ Method
def writeToBQ(documentEntities: dict, table_id: str):
    print("Inserting into BQ ************** ")
    #Insert into BQ    
    client = bigquery.Client()    
    table = client.get_table(table_id)

    print ('Adding the row')
    rows_to_insert= [documentEntities]

    print (' ********** NEW Row Column: ',rows_to_insert)
    errors = client.insert_rows_json(table, rows_to_insert) 
    if errors == []:
        print("New rows have been added.") 
    else:
        print ('Encountered errors: ',errors)
        
# Extract shards from the text field
def get_text(doc_element: dict, document: dict):
    """
    Document AI identifies form fields by their offsets
    in document text. This function converts offsets
    to text snippets.
    """
    response = ""
    # If a text segment spans several lines, it will
    # be stored in different text segments.
    for segment in doc_element.text_anchor.text_segments:
        start_index = (
            int(segment.start_index)
            if segment in doc_element.text_anchor.text_segments
            else 0
        )
        end_index = int(segment.end_index)
        response += document.text[start_index:end_index]
    return response
def download_blob(bucket_name, source_blob_name):
    """Downloads a blob from the bucket."""
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(source_blob_name)

    return blob
