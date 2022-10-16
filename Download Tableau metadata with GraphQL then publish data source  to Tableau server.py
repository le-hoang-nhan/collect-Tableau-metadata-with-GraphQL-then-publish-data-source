import os
from pathlib import Path
from dotenv import load_dotenv
import tableauserverclient as TSC
import pandas as pd
import json


#access to Tableau
tableau_auth = TSC.TableauAuth('......')
server = TSC.Server('https://...........', use_server_version=True)
request_options = TSC.RequestOptions(pagesize=1000)
server.auth.sign_in(tableau_auth)

#GraphiQL querry
query = """

{
  workbooks   {
    workbook_name: name
    upstreamDatasources {
      upstr_ds_name: name
    }
    embeddedDatasources {
      emb_ds_name: name
    }
    upstreamFlows {
      flow_name: name
    }
  }
}

    """

with server.auth.sign_in(tableau_auth):
    #Query the Metadata API and store the response in resp
    resp = server.metadata.query(query)
    datasources = resp['data']['workbooks']

with open(r'G:\....\linked Workbook-Data source-Calculated field/workbook-lineage.json', 'w') as fp:
    json.dump(datasources, fp)

#GraphiQL querry
query = """

{
 workbooks {
  name
  embeddedDatasources {
   name
   fields {
    ... on CalculatedField {
     name
     formula
    	}
   	}
  	}
	}
}

    """

with server.auth.sign_in(tableau_auth):
    #Query the Metadata API and store the response in resp
    resp = server.metadata.query(query)
    datasources = resp['data']['workbooks']

with open(r"G:....\linked Workbook-Data source-Calculated field\calculated fields.json", 'w') as fp:
    json.dump(datasources, fp)

#GraphiQL querry
query = """

{
  flows    {
    flow_name: name
    upstreamDatasources {
      upstr_ds_name: name
    }

    upstreamFlows {
      upstr_flow_name: name
    }
    downstreamFlows 
     {
      downstr_flow_name: name
    }
    
    downstreamDatasources {
      downstr_ds_name: name
    }
    
    downstreamWorkbooks {
      downstr_wb_name: name
    }
  }
}


    """

with server.auth.sign_in(tableau_auth):
    #Query the Metadata API and store the response in resp
    resp = server.metadata.query(query)
    datasources = resp['data']['flows']

with open(r'G:\...\linked Workbook-Data source-Calculated field/workflow-lineage.json', 'w') as fp:
    json.dump(datasources, fp)

#['05_Adhoc analyses', '67115608-8e83-4486-a340-88ee180fdc60']
YOUR_WORKBOOK_FILE_PATH = r"G:\....\linked Workbook-Data source-Calculated field\Info for workflows - data sources - flows.twbx"
YOUR_DB_SERVER_ADDRESS = 'https://....'
YOUR_PROJECT_ID = "671156....."
YOUR_WORKBOOK_NAME = "Info for workflows - data sources - flows"

with server.auth.sign_in(tableau_auth):
   # create a workbook item
   wb_item = TSC.WorkbookItem(name=YOUR_WORKBOOK_NAME, project_id=YOUR_PROJECT_ID)
   # call the publish method with the workbook item
   wb_item = server.workbooks.publish(wb_item, YOUR_WORKBOOK_FILE_PATH, 'Overwrite', skip_connection_check = True)