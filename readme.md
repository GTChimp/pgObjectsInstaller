# pgObjectsInstaller

pgObjectsInstaller is a deployment automation tool for Postgresql database objects.


# Installation/Build

The source code is written on Python version 3.11 but also some previous versions can be viable.
To build a win exe all you need is download/clone source, install the requirements to your project in IDE and run something like 
```
pyinstaller postgres_builder.py --distpath '%userprofile%/Desktop/atata' --clean --workpath '%userprofile%/Desktop/atata/build' --add-data "default_properties.json:." --add-data "misc:misc"
```
in the IDE's console(100% works with PyCharm, can vary if you use something else or wanna use another libraries, here the PyInstaller is used)

# Quick start

The default version of the app requires that your Postgresql repository has structure like here: [pg_dummydb](https://github.com/GTChimp/pg_dummydb), i.e. mandatory elements are 2 catalogs: OBJ - folder, containing your db structure represented as .sql files; Requests - folder, containing subfolders each of which should be named as ticket(Jira,Trello etc) and contain objects.inst file with list of objects to deploy.
Also, the general recommendation is that the user/role used for the deployment was an owner of the database.


# Default properties

Default properties for the app are represented in the default_properties.json file.
```
{  
  "repo": {  
    "remote_path": "https://github.com/GTChimp/pg_dummydb.git",  
  "local_path": {  
      "env": "userprofile",  
  "path": "/Desktop/pg_repo"  
  },  
  "branch": "master",  
  "folder": "init"  
  },  
  "db": {  
    "host": "localhost",  
  "port": 5432,  
  "dbname": "test_db",  
  "user": "tester"  
  },
  "log_table": "main.log_ci_results"  
}
```
Key *repo* represents git repository properties needed for cloning and composing objects to deploy.

 - *remote_path* - url of your Postgresql repo
 - *local_path* - path whither repo should be cloned, *env* - environment variable(set null if not needed), *path* - path to destination folder
 - *branch* - branch name or commit SHA-1
 - *folder* - name of the subfolder of Requests catalog

Key *db* represents database connection properties.

Key *log_table* stores name of the ci log table. It must be present in your database and have structure like [here](https://github.com/GTChimp/pg_dummydb/blob/master/OBJ/Schemas/main/Tables/log_ci_results.sql)

# Misc options
#### List of additinal options

 - at propmpts time here is a possibility to chose deploy mode, i.e. deploy all your .sql scripts as single statement  or separately. The default is separate mode. In order for the "one statement" mode to function correctly, your DDLs and PL/pgSQL statements must have tagged dollar quoting.

# Notes

 - for now supported only UTF-8 files encoding
 - if your repository requires additional authentication(organization's policies etc.) and the credentials aren't stored in credentials manager you can get error trying to clone the repo first time