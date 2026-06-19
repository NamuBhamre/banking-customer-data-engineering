# My Project Scenario-Banking Customer Domain

- Imported 10 CSV files containing banking customer data from our pc to SQL server using SSMS to cloud.
         
<img width="923" height="609" alt="image" src="https://github.com/user-attachments/assets/d5deda0e-f8a0-4609-b10c-4d529262167d" />

-Creating resource group

<img width="953" height="486" alt="image" src="https://github.com/user-attachments/assets/00798075-57b7-4322-bfa1-6b56755afdb9" />

-Migrating data from on premises SQL server to ADF.

<img width="940" height="609" alt="image" src="https://github.com/user-attachments/assets/cec255ff-171d-44c5-8306-8371b03ca392" />

<img width="838" height="709" alt="image" src="https://github.com/user-attachments/assets/c43a90af-954e-454b-b70a-083d6a875a09" />

-Creating IR(Self hosted integration runtime) which will be downloaded while creating IR. 
                 Two IR’s-1. Self hosted Integration Runtime. Local-mssql-ir
                          2. AutoResolvedIntegrationRuntime
-linked service-for connecting on premises sql server to cloud->source_dataset
-For each activity in pipeline, there will be two linked services and two datasets.
-One for source(input) and another for sink(output)

<img width="940" height="613" alt="image" src="https://github.com/user-attachments/assets/8f4f733a-027b-4cb8-a8f3-98618da58862" />

<img width="940" height="702" alt="image" src="https://github.com/user-attachments/assets/ee75485e-2114-4017-a428-d0a364b2b6a4" />

-I have created one pipeline SQLmigiration where activity->Lookup lookup Tables(name can be changed in general)

<img width="940" height="473" alt="image" src="https://github.com/user-attachments/assets/e5bafcf6-2452-40a6-b21e-e408335444e4" />

<img width="939" height="478" alt="image" src="https://github.com/user-attachments/assets/38a38d9b-7931-49da-82d2-933a619dad84" />

<img width="940" height="350" alt="image" src="https://github.com/user-attachments/assets/3965d62e-b781-4f1d-af70-8511c638c0ed" />

<img width="941" height="457" alt="image" src="https://github.com/user-attachments/assets/d0f73728-900a-497c-8163-3efc7176187e" />

<img width="941" height="515" alt="image" src="https://github.com/user-attachments/assets/4b6c2300-4a88-46ae-bf6a-c0d66a00142d" />

<img width="940" height="735" alt="image" src="https://github.com/user-attachments/assets/5fa9b961-e516-48c9-a33e-bf61f2144e9f" />

- @activity(Lookup up Tables).output.value->It is used for dynamic content.
On success of lookup activity we can attach it by selecting new activity for each activity and renamed it general as ForEach Each Table.

<img width="940" height="577" alt="image" src="https://github.com/user-attachments/assets/62312bae-6434-430e-915c-cc9f40799e05" />

- By using add activity to for each activity create copy data activity.

- In this, I created one more activity which is run on copy Batch Table by using for each first inside this for each again copying each table from ssms at the same by passing parameters as file names.

<img width="940" height="686" alt="image" src="https://github.com/user-attachments/assets/96ced606-0e7e-40a0-a1ac-52bf4a2f8593" />

<img width="941" height="719" alt="image" src="https://github.com/user-attachments/assets/0c919123-26e2-4ad1-b660-c8d0ec0c54c3" />

<img width="914" height="815" alt="image" src="https://github.com/user-attachments/assets/454f9737-2a98-4e4e-9a05-7a9c0cd3fc30" />

<img width="940" height="512" alt="image" src="https://github.com/user-attachments/assets/e5239638-ce30-497c-9cb6-858cb0280b14" />

<img width="982" height="575" alt="image" src="https://github.com/user-attachments/assets/39ae98ec-00d3-49e2-a9e2-83bfd85afd2d" />

<img width="940" height="927" alt="image" src="https://github.com/user-attachments/assets/bb15eaf4-78c8-4844-a18e-b972baafdd08" />

<img width="940" height="621" alt="image" src="https://github.com/user-attachments/assets/bc30cc7f-ec0b-44cb-ad89-8a7971c63783" />

<img width="940" height="621" alt="image" src="https://github.com/user-attachments/assets/7c536c36-a48a-442f-beff-aa28fd965f3a" />

<img width="940" height="675" alt="image" src="https://github.com/user-attachments/assets/f4338f36-803d-4b32-b4b2-94ed0d92198f" />

<img width="940" height="1119" alt="image" src="https://github.com/user-attachments/assets/636668e9-7227-40bd-8498-f8e64ff601a2" />

<img width="940" height="1117" alt="image" src="https://github.com/user-attachments/assets/af71fd6c-b919-45bb-912d-d815940c6852" />

- For copying data, we created one container inside data lake storage with the name bronze. So, all raw data will be copied to bronze.
- Finally, all data tables get copied from on premises sql server to adls using this copy data activity in adf but for adls we used auto created IR which is for the services inside cloud. And self hosted IR for data migiration from outside cloud.
- Data will be copied to a bronze container in datalake.
  
## Connecting adls to databricks 
- First created app registration for client id, tenant id, secret(certificate) then give an access in datalake’s access control->assign role->to app for access in datalakes data, Created Key vaults and assign to secrets, Added secret to key-vault, created secret scope in databricks to securely access adls data in azure databricks.

<img width="577" height="388" alt="image" src="https://github.com/user-attachments/assets/47202f5b-8049-4008-b149-5e9e11002f89" />

-Created a one notebook in azure databricks for service principle to connect with adls and write below code.(first checking spark config)

# COMMAND 

secret_id=""
secret_value=""
client_id=""
tenant_id=""

(not showing values for safety.)

# COMMAND 

configs = {"fs.azure.account.auth.type": "OAuth",
          "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
          "fs.azure.account.oauth2.client.id": dbutils.secrets.get(scope="Batch22_NRB_Scope", key="appid"),
          "fs.azure.account.oauth2.client.secret": dbutils.secrets.get(scope="Batch22_NRB_Scope", key="secret"),
          "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{dbutils.secrets.get(scope='Batch22_NRB_Scope', key='tenantid')}/oauth2/token"}

# COMMAND 

dbutils.fs.mount(
		source = "abfss://bronze@nrbdatalake1.dfs.core.windows.net/",
		  mount_point = "/mnt/bronze",
		  extra_configs = configs)

# COMMAND

dbutils.fs.ls("/mnt/bronze")

# COMMAND 

dbutils.fs.ls("/mnt/bronze/dbo")

# COMMAND

dbutils.fs.mount(
		source = "abfss://silver@nrbdatalake1.dfs.core.windows.net/",
		  mount_point = "/mnt/silver",
		  extra_configs = configs)

# COMMAND 

dbutils.fs.mount(
		source = "abfss://gold@nrbdatalake1.dfs.core.windows.net/",
		  mount_point = "/mnt/gold",
		  extra_configs = configs)

## Databricks Notebooks
      
- Loading data in first_notebook_NRB in databricks from bronze container in adls.
- I have applied minor transformations to customer data and employee data both by creating a mounting point to  bronze in first_notebook_NRB and getting it stored inside a silver container again using a mounting point.
- Open second_notebook_NRB and apply joins, windows functions and aggregate functions on customer data stored in a gold container using third_notebook_NRB. 
- Created pipeline in adf for triggering notebooks using scheduled trigger.
  
<img width="914" height="558" alt="image" src="https://github.com/user-attachments/assets/5aa9c546-81c1-470a-b131-bdd3fe663613" />

---
Author: Namrata Ratilal Bhamre  
© 2026 Namrata Ratilal Bhamre. All rights reserved.




