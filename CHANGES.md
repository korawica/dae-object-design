Changelog
=========

Version x.0.0
-------------

* [ ] Create deployment for move to py38, py310, py311, ...

Version 1.0.0
-------------

---

TODO Lists
----------

* [ ] Filter asset version in the base RDBMS connection object

Pre-Version 0.0.2
-----------------

* [ ] Fix converter component
    * [ ] Fix `Statement` Converter object
    * [ ] Fix `Schema` Converter object
    * [x] Fix `CronJob` Converter object
  
* [ ] Fix loader object can use control parameters

Pre-Version 0.0.1
-----------------

* [ ] Create objects for loader component
    * [ ] Create base connection objects
        * [ ] Add local file system connection
        * [ ] Add S3 file system connection
        * [ ] Add Blob file system connection
    * [ ] Create base catalog objects
        * [ ] Add Pandas object
    * [ ] Create base action object    
    * [ ] Create base node object

* [ ] Create loader component
    * [x] Create base loader object
    * [x] Create `Connection` Loader
    * [x] Create `Catalog` Loader
    * [x] Create `Node` Loader
    * [x] Create `Schedule` Loader
    * [ ] ~~Create `Pipeline` Loader~~
  
* [x] Create register component
    * [x] Create base register object
    * [x] Create `Register` object
    * [x] Create metadata process with config object
  
* [ ] Create formatter component
    * [x] Create `Datetime` format object
    * [x] Create `Version` format object
    * [x] Create `Serial` format object

* [ ] Create config base component
    * [x] Create metadata object
    * [x] Create logging object
    * [x] Create base config loader object
  
* [x] Create I/O object for read and write file with any types
    * [x] Add `.yaml` type
    * [x] Add `.csv` type    
    * [x] Add `.json` type
    * [x] Add `.pickle` type
    * [x] Add `.env` type
    * [x] Add `.marshal` type
    * [ ] ~~Add `.msgpack` type~~
    * [ ] ~~Add `.xlsx` type~~
    * [ ] ~~Add `.html` type~~
    * [ ] ~~Add `.hdf5` type~~
    * [ ] ~~Add `.orc` type~~
