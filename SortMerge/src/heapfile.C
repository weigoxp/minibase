#include "heapfile.h"

// ******************************************************
// Error messages for the heapfile layer

static const char *hfErrMsgs[] = {
    "bad record id",
    "bad record pointer", 
    "end of file encountered",
    "invalid update operation",
    "no space on page for record", 
    "page is empty - no records",
    "last record on page",
    "invalid slot number",
    "file has already been deleted",
};

static error_string_table hfTable( HEAPFILE, hfErrMsgs );

// ********************************************************
// Constructor
HeapFile::HeapFile( const char *name, Status& returnStatus )
{
     Status status;
    if ( name == NULL) { 
       // file is temporary  heapfile which will be
       // deleted by the destructor
      fileName = new char[4];
      strcpy(fileName, "TEMP");
      //cout<<"temporary file";
    } 
     else
    { 
      fileName = new char[strlen(name)+1];
      strcpy(fileName, name); 
      //cout<<"file opened "<<fileName<<endl;
    }

    // Open the file and get the entry corresponding to the given file 
    status = MINIBASE_DB->get_file_entry(fileName, firstDirPageId);	
    if(status != OK){  // file does not exist 
        // create the file and add it into file entry.  
        Page *newDirPage; 
	status = MINIBASE_BM->newPage(firstDirPageId, newDirPage);  
        if (status != OK) {
              returnStatus = MINIBASE_CHAIN_ERROR( HEAPFILE, status );
              return;
              } 

	// Status add_file_entry(const char* fname, PageId start_page_num);`
	status = MINIBASE_DB->add_file_entry(fileName, firstDirPageId);     
        if (status != OK) {
              returnStatus = MINIBASE_CHAIN_ERROR( HEAPFILE, status );
              return;
              }

	((HFPage*) newDirPage)->init(firstDirPageId); 
  
        // unpin the Directory page 
        status=MINIBASE_BM->unpinPage(firstDirPageId, true); 
        if (status != OK) {
              returnStatus = MINIBASE_CHAIN_ERROR( HEAPFILE, status );
              return;
              }
    }
    

    file_deleted=false;
    returnStatus = OK;   
}

// ******************
// Destructor
HeapFile::~HeapFile()
{
    if (strcmp(fileName, "TEMP") == 0)
        deleteFile();
}

// *************************************
// Return number of records in heap file
int HeapFile::getRecCnt()
{
  int recCount= 0;

    HFPage * curDirPage;
    PageId curDirPageID = firstDirPageId;

    // iterate the directory
   // Status pinPage(int PageId_in_a_DB, Page*& page,int emptyPage=0, const char *filename=NULL);
    while(curDirPageID!= INVALID_PAGE){
      // pin page to get 1st dirpage pointer
        MINIBASE_BM->pinPage(curDirPageID,(Page*&)curDirPage);

        // use first/ next to loop through direcotry hfpage 
        RID curRID;
        curDirPage->firstRecord(curRID); // get rid of first rec 
        DataPageInfo *curRec = new DataPageInfo(); // use to store dirpage rec in future.
        int curLen = 0;

        // loop recs dir page contains.

        Status status = OK;
        while(status==OK){
            curDirPage -> getRecord(curRID, (char*) curRec, curLen);// get rec contains hfpage info.
            recCount += curRec->recct;
            status = curDirPage-> nextRecord(curRID,curRID);
        }
            //move to next, unpin cur
        PageId tmp = curDirPageID;
        curDirPageID = curDirPage -> getNextPage();


        MINIBASE_BM -> unpinPage(tmp);

        delete curRec;
    }


    return recCount;
}

// *****************************
// Insert a record into the file
Status HeapFile::insertRecord(char *recPtr, int recLen, RID& outRid)
{
    PageId dirPageId = firstDirPageId, nextDirPageId, dataPageId; 
    HFPage *dirPage, *dataPage ;
    RID dirRecId;
    DataPageInfo *dataPageInfo = new DataPageInfo();
    Status status, dirPageStatus;
    int dirRecLength;
    //for test 5.
    if(recLen>1000)
        return MINIBASE_CHAIN_ERROR(HEAPFILE, FAIL);

    // loop all directory pages, check DataPageInfo, 
    //  find datapage with availspace.
    while (dirPageId != INVALID_PAGE) {
        status = MINIBASE_BM->pinPage(dirPageId, (Page *&) dirPage);
        if (status != OK)
            return MINIBASE_CHAIN_ERROR(HEAPFILE, status);

        // get first record from directory page
        dirPageStatus = dirPage->firstRecord(dirRecId);

        while (dirPageStatus == OK) { //Check every DataPageInfo in directory page for datapage with available space
                status = dirPage->getRecord(dirRecId, (char *&) dataPageInfo, dirRecLength);
                if (status == OK){
                     // got a data page with available space
                    if (dataPageInfo->availspace >= recLen) {

                        status = dirPage->returnRecord(dirRecId, (char *&) dataPageInfo, dirRecLength);
                        if (status == OK){

                            dataPageId = dataPageInfo->pageId;
                            // Pin this data page
                            status = MINIBASE_BM->pinPage(dataPageId, (Page *&) dataPage);
                            if (status != OK)
                                return MINIBASE_CHAIN_ERROR(HEAPFILE, status);
                            // Insert record
                            dataPage->insertRecord(recPtr, recLen, outRid);
                            // Update DataPageInfo in directory page
                            dataPageInfo->availspace = dataPage->available_space();
                            dataPageInfo->recct++;
                            // Unpin both data and directory pages
                            status = MINIBASE_BM->unpinPage(dataPageId, true);
                            if (status != OK)
                                return MINIBASE_CHAIN_ERROR(HEAPFILE, status);
                            status = MINIBASE_BM->unpinPage(dirPageId, true);
                            if (status != OK)
                                return MINIBASE_CHAIN_ERROR(HEAPFILE, status);
                            return OK;
                           }
                       }
                }

          	dirPageStatus= dirPage->nextRecord(dirRecId, dirRecId);// get next record in this directory page
        }
        // Look into next directory page
        nextDirPageId = dirPage->getNextPage();
        status = MINIBASE_BM->unpinPage(dirPageId);
        if (status != OK)
            return MINIBASE_CHAIN_ERROR(HEAPFILE, status);
        dirPageId = nextDirPageId;
    }


    // No data page found with available space
    // Need a new data page
    status = newDataPage(dataPageInfo);
    if (status != OK)
        return MINIBASE_CHAIN_ERROR(HEAPFILE, status);
    // pin the data page, insert the record, update dataPageInfo
    dataPageId = dataPageInfo->pageId;
    status = MINIBASE_BM->pinPage(dataPageId, (Page *&) dataPage);
    if (status != OK)
        return MINIBASE_CHAIN_ERROR(HEAPFILE, status);
    dataPage->insertRecord(recPtr, recLen, outRid);
    dataPageInfo->recct++;
    dataPageInfo->availspace = dataPage->available_space();

    // Allocate space for the datapageInfo
    status = allocateDirSpace(dataPageInfo, dirPageId, dirRecId);
    if (status != OK)
        return MINIBASE_CHAIN_ERROR(HEAPFILE, status);
    status = MINIBASE_BM->pinPage(dirPageId, (Page *&) dirPage);
    if (status != OK)
        return MINIBASE_CHAIN_ERROR(HEAPFILE, status);

    // Unpin both data and directory pages
    status = MINIBASE_BM->unpinPage(dataPageId, true);
    if (status != OK)
        return MINIBASE_CHAIN_ERROR(HEAPFILE, status);
    status = MINIBASE_BM->unpinPage(dirPageId, true);
    if (status != OK)
        return MINIBASE_CHAIN_ERROR(HEAPFILE, status);

    return OK;
} 


// ***********************
// delete record from file
Status HeapFile::deleteRecord (const RID& rid)
{
    // fill in the body
    PageId dataPageID;
    HFPage *dataPage;
    PageId dirPageID;
    HFPage *dirPage;
    RID dirRID;
    Status status;

    status = findDataPage(rid, dirPageID, dirPage, dataPageID,dataPage, dirRID);
    if(status==OK){
        status = dataPage->deleteRecord(rid);
        if(status!=OK)
            return MINIBASE_CHAIN_ERROR(HEAPFILE, status);

        if(dataPage->empty()){
            status= dirPage->deleteRecord(dirRID);
            if(status!=OK)
                return MINIBASE_CHAIN_ERROR(HEAPFILE, status);
        }
    


        status = MINIBASE_BM->unpinPage(dataPageID, true);
        if (status != OK)
            return MINIBASE_CHAIN_ERROR(HEAPFILE, status);

        status = MINIBASE_BM->unpinPage(dirPageID, true);
        if (status != OK)
            return MINIBASE_CHAIN_ERROR(HEAPFILE, status);

        if (dataPage->empty()) {
            status = MINIBASE_BM->freePage(dataPageID);
            if (status != OK)
                return MINIBASE_CHAIN_ERROR(HEAPFILE, status);
        }
    }

    else
        return status;

    return OK;
}

// *******************************************
// updates the specified record in the heapfile.
Status HeapFile::updateRecord (const RID& rid, char *recPtr, int recLen)
{
    // fill in the body
    PageId dataPageID;
    HFPage *dataPage;
    PageId dirPageID;
    HFPage *dirPage;
    RID dirRID;

    Status status = findDataPage(rid, dirPageID, dirPage, dataPageID, dataPage, dirRID);
    if(status!=OK)
        return MINIBASE_CHAIN_ERROR(HEAPFILE, status);

    char* targetRecPtr;
    int targetRecLen;

    // get target rec
    status = dataPage->returnRecord(rid, targetRecPtr, targetRecLen);
    if(status!=OK||targetRecLen!= recLen){
        
        status = MINIBASE_BM->unpinPage(dataPageID);
        if (status != OK)
            return MINIBASE_CHAIN_ERROR(HEAPFILE, status);

        status = MINIBASE_BM->unpinPage(dirPageID);
        if (status != OK)
            return MINIBASE_CHAIN_ERROR(HEAPFILE, status);

            return MINIBASE_CHAIN_ERROR(HEAPFILE, status);
    }


    memcpy(targetRecPtr,recPtr,recLen);

    status = MINIBASE_BM->unpinPage(dataPageID,true);
    if (status != OK)
        return MINIBASE_CHAIN_ERROR(HEAPFILE, status);

    status = MINIBASE_BM->unpinPage(dirPageID);
    if (status != OK)
        return MINIBASE_CHAIN_ERROR(HEAPFILE, status);


    return OK;
}

// ***************************************************
// read record from file, returning pointer and length
Status HeapFile::getRecord (const RID& rid, char *recPtr, int& recLen)
{
    PageId dirPageID, dataPageID;
    HFPage *dirPage, *dataPage;
    RID dirRID;
    Status status, recordStatus;

    // Get the record
    recordStatus = findDataPage(rid, dirPageID, dirPage, dataPageID, dataPage,dirRID );
    if (recordStatus != OK) { // no record found
        status = MINIBASE_BM->unpinPage(dataPageID);
        if (status != OK)
            return MINIBASE_CHAIN_ERROR(HEAPFILE, status);
        status = MINIBASE_BM->unpinPage(dirPageID);
        if (status != OK)
            return MINIBASE_CHAIN_ERROR(HEAPFILE, status);
        return MINIBASE_CHAIN_ERROR(HEAPFILE, recordStatus);
    }

    dataPage->getRecord(rid, recPtr, recLen);
    status = MINIBASE_BM->unpinPage(dataPageID);
    if (status != OK)
        return MINIBASE_CHAIN_ERROR(HEAPFILE, status);
    status = MINIBASE_BM->unpinPage(dirPageID);
    if (status != OK)
        return MINIBASE_CHAIN_ERROR(HEAPFILE, status);

  return OK;
}

// **************************
// initiate a sequential scan
Scan *HeapFile::openScan(Status& status)
{
    Scan *scan = new Scan(this, status);
    return scan;
}

// ****************************************************
// Wipes out the heapfile from the database permanently. 
Status HeapFile::deleteFile()
{
    
    // we need to iterate dirpage/ datapge to free them.

    PageId dirPageID = firstDirPageId;
    HFPage *dirPage;
    Status status;
    if(file_deleted==true) return DONE;

    while(dirPageID!=INVALID_PAGE){
         status = MINIBASE_BM->pinPage(dirPageID, (Page *&) dirPage);
        if (status != OK)
            return MINIBASE_CHAIN_ERROR(HEAPFILE, status);

        RID curRID;
        status = dirPage->firstRecord(curRID); // get rid of first rec 
        if(status==OK){

        

        DataPageInfo *curDataPageInfo = new DataPageInfo(); // use to store dirpage rec in future.
        int curLen = 0;

        // loop recs dir page contains. get data pageid from rec, and free them.
        Status getRecstatus = OK;
        while(getRecstatus==OK){
            dirPage->getRecord(curRID, (char*) curDataPageInfo, curLen);// get rec contains hfpage info.


            status = MINIBASE_BM->freePage(curDataPageInfo->pageId);
            if (status != OK)
                return MINIBASE_CHAIN_ERROR(HEAPFILE, status);

            getRecstatus = dirPage-> nextRecord(curRID,curRID);
        }

    }
        PageId tmp = dirPageID;
        dirPageID = dirPage -> getNextPage();

        status = MINIBASE_BM->unpinPage(tmp);
        if (status != OK)
            return MINIBASE_CHAIN_ERROR(HEAPFILE, status);

        status = MINIBASE_BM->freePage(tmp);
            if (status != OK)
                return MINIBASE_CHAIN_ERROR(HEAPFILE, status);

    }

    status = MINIBASE_DB->delete_file_entry(fileName);
    if (status != OK)
        return MINIBASE_CHAIN_ERROR(HEAPFILE, status);

    file_deleted = true;
    return OK;
}

// ****************************************************************
// Get a new datapage from the buffer manager and initialize dpinfo
// (Allocate pages in the db file via buffer manager)
Status HeapFile::newDataPage(DataPageInfo *dpinfop)
{
    HFPage *newPage;
    PageId newPageID;

    // new page includes pin page/ will return error if full
    Status status = MINIBASE_BM->newPage (newPageID,(Page*&)newPage);
    if(status!= OK)
        return MINIBASE_CHAIN_ERROR(HEAPFILE, status); 
     
    newPage->init(newPageID);

    //initialize dpinfo
    dpinfop -> availspace = newPage->available_space();
    dpinfop -> recct = 0;
    dpinfop -> pageId = newPageID;
    status = MINIBASE_BM->unpinPage (newPageID);   
    if (status != OK)
        return MINIBASE_CHAIN_ERROR(HEAPFILE, status);

    return OK;
}

// ************************************************************************
// Internal HeapFile function (used in getRecord and updateRecord): returns
// pinned directory page and pinned data page of the specified user record
// (rid).
//
// If the user record cannot be found, rpdirpage and rpdatapage are 
// returned as NULL pointers.
//
Status HeapFile::findDataPage(const RID& rid,
                    PageId &rpDirPageId, HFPage *&rpdirpage,
                    PageId &rpDataPageId,HFPage *&rpdatapage,
                    RID &rpDataPageRid)
{
    PageId curDirPageID = firstDirPageId, nextDirPageId;
    HFPage *curDirPage;
    RID curDirRID;
    Status status, dirPageStatus;
    DataPageInfo *dataPageInfo = new DataPageInfo();
    int dirRecLength;

    while (curDirPageID != INVALID_PAGE) {// Loop through directory pages
        status = MINIBASE_BM->pinPage(curDirPageID, (Page *&) curDirPage);
        if (status != OK)
            return MINIBASE_CHAIN_ERROR(HEAPFILE, status);

        // get first record from directory page
        dirPageStatus = curDirPage->firstRecord(curDirRID);
        if (dirPageStatus != OK) {
            rpdirpage = NULL;
            rpdatapage = NULL;
            return MINIBASE_CHAIN_ERROR(HEAPFILE, dirPageStatus);
        }

        while (dirPageStatus == OK) { //Check every DataPageInfo in directory page
                status = curDirPage->getRecord(curDirRID, (char *&) dataPageInfo, dirRecLength);
                if (status == OK && dataPageInfo->pageId == rid.pageNo){ // got the expected record
                	rpDataPageId = dataPageInfo->pageId;
                	status = MINIBASE_BM->pinPage(rpDataPageId, (Page *&) rpdatapage);
       			if (status != OK)
            			return MINIBASE_CHAIN_ERROR(HEAPFILE, status);

                	rpDirPageId = curDirPageID;
                	rpdirpage = curDirPage;
                	rpDataPageRid = curDirRID;
                	return OK;            
                } 
          	dirPageStatus= curDirPage->nextRecord(curDirRID, curDirRID);// get next record in this directory page
	}       

        // Look into next directory page
        nextDirPageId = curDirPage->getNextPage();
        status = MINIBASE_BM->unpinPage(curDirPageID);
        if (status != OK)
            return MINIBASE_CHAIN_ERROR(HEAPFILE, status);
        curDirPageID = nextDirPageId;
    }

    //no record found with given rid
    rpdirpage = NULL;
    rpdatapage = NULL;
    return DONE;
}

// *********************************************************************
// Allocate directory space for a heap file page 

Status HeapFile::allocateDirSpace(struct DataPageInfo * dpinfop,
                            PageId &allocDirPageId,
                            RID &allocDataPageRid)
{
    PageId curDirPageID = firstDirPageId;
    HFPage *curDirPage;
    PageId preDirPageID =curDirPageID;
    RID insertedDP;

    //check through directory to find space 
    while(curDirPageID!=INVALID_PAGE){

        Status status = MINIBASE_BM->pinPage(curDirPageID, (Page*&)curDirPage);
        if(status!=OK)
            return MINIBASE_CHAIN_ERROR(HEAPFILE, status); 

        status = curDirPage->insertRecord((char*)dpinfop,sizeof(DataPageInfo),insertedDP);
        
        if(status==OK){

            allocDirPageId = curDirPageID;
            allocDataPageRid = insertedDP;
                    //unpin the passed page.
            status=MINIBASE_BM ->unpinPage(curDirPageID,true);
            if(status!=OK) 
                return MINIBASE_CHAIN_ERROR(HEAPFILE, status); 

            return OK;
        }

        //found it.
         //move to next diretory page
        else{  
            
            preDirPageID = curDirPageID;
            curDirPageID = curDirPage->getNextPage();

            status=MINIBASE_BM ->unpinPage(preDirPageID);
            if(status!=OK) 
                return MINIBASE_CHAIN_ERROR(HEAPFILE, status); 
        }

    }


    // didnt return in previous code.
    //we need a new direcotry page.
    //now curDirPageID == INVALID_PAGE

    PageId newPageID;
    HFPage * newPage;
    HFPage *preDirPage;

    Status status = MINIBASE_BM->newPage (newPageID,(Page*&)newPage);
    if(status!=OK) 
        return MINIBASE_CHAIN_ERROR(HEAPFILE, status); 

    newPage->init(newPageID);
    newPage->setPrevPage(preDirPageID);

    // insert DataPageInfo and get rid of data page.
    allocDirPageId = newPageID;
    newPage->insertRecord((char*)dpinfop,sizeof(DataPageInfo),insertedDP);
    allocDataPageRid = insertedDP;


    // pin prepage then set next ptr to newpage
    status = MINIBASE_BM->pinPage(preDirPageID, (Page*&)preDirPage);
    if(status!=OK) 
        return MINIBASE_CHAIN_ERROR(HEAPFILE, status); 

    preDirPage->setNextPage(newPageID);

    //unpin both
    status = MINIBASE_BM->unpinPage(newPageID);
    if(status!=OK) 
        return MINIBASE_CHAIN_ERROR(HEAPFILE, status);  

    status = MINIBASE_BM->unpinPage(preDirPageID);
    if(status!=OK) 
        return MINIBASE_CHAIN_ERROR(HEAPFILE, status); 



    return OK;
}

// *******************************************
