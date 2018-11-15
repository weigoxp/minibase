/*****************************************************************************/
/*************** Implementation of the Buffer Manager Layer ******************/
/*****************************************************************************/

using namespace std; 
#include "buf.h"

// Define buffer manager error messages here
//enum bufErrCodes  {...};

// Define error message here
static const char* bufErrMsgs[] = { 
  // error message strings go here
  "Not enough memory to allocate hash entry",
  "Inserting a duplicate entry in the hash table",
  "Removing a non-existing entry from the hash table",
  "Page not in hash table",
  "Not enough memory to allocate queue node",
  "Poping an empty queue",
  "OOOOOOPS, something is wrong",
  "Buffer pool full",
  "Not enough memory in buffer manager",
  "Page not in buffer pool",
  "Unpinning an unpinned page",
  "Freeing a pinned page"
};

// Create a static "error_string_table" object and register the error messages
// with minibase system 
static error_string_table bufTable(BUFMGR,bufErrMsgs);
//*************************************************************
//** Helper Hash function.
//************************************************************
int hashfunction(PageId pageNo){
    int a =23;
    int b = 31;

    int entryToBucket = (a* pageNo +b) % HTSIZE;
    return entryToBucket;

}

//___________________________________________________________________

//*************************************************************
//** This is the implementation of BufMgr
//************************************************************

BufMgr::BufMgr (int numbuf, Replacer *replacer) {
    numBuffers = numbuf;
    bufPool = new Page[numbuf];
    bufDescr = new Descriptor[numbuf];

    hashtable = new vector<hashpair>[7];

    for (int i = 0; i < numbuf; i++) {
        bufDescr[i].pageNumber = INVALID_PAGE;
    }


}

//*************************************************************
//** This is the implementation of ~BufMgr
//************************************************************
BufMgr::~BufMgr(){
  // put your code here
    flushAllPages();
    delete[] bufPool;
    delete[] bufDescr;
    delete[] hashtable;
}

//*************************************************************
//** This is the implementation of pinPage
//************************************************************
Status BufMgr::pinPage(PageId PageId_in_a_DB, Page*& page, int emptyPage) {
//cout<<"\nI am in pin page."<<endl;
    Status status;
    unsigned int i;
    int iReplace=-1;
    int entry = hashfunction(PageId_in_a_DB);

    vector<hashpair>::iterator it;

    for( it = hashtable[entry].begin(); it != hashtable[entry].end(); ++it){

          if(it->pageNumber == PageId_in_a_DB){ 
              int i = it->frameNumber;

              bufDescr[i].pin_count++;
              page = &bufPool[i];
              return OK;
            }
      //this page is already in buffer pool
      //frameNum = i;  
    }

    i=0;

    while(i < numBuffers){
        if (bufDescr[i].pageNumber == INVALID_PAGE) { 
        //found an empty slot in the buffer pool
        //cout<<"\nPageId_in_a_DB= "<<PageId_in_a_DB<<endl;
            status = MINIBASE_DB->read_page(PageId_in_a_DB, &bufPool[i]);
            if (status != OK)
                return MINIBASE_CHAIN_ERROR(BUFMGR, status);
            // set values for the frame in Descriptor
            bufDescr[i].pageNumber = PageId_in_a_DB;
            bufDescr[i].pin_count = 1;
            bufDescr[i].dirtybit = false;
            bufDescr[i].love = 0;
            bufDescr[i].hate = 0;

            // Point the page at the location in the buffer pool
            page = &bufPool[i];

            //update hashtable.
            struct hashpair p;

            p.pageNumber = PageId_in_a_DB;
            p.frameNumber = i;
            hashtable[entry].push_back(p);

	          return OK;
	       }

        i++;
        }

    // i = numBuffers   
    // buffer does not have any empty slots
    // find a frame to replace it's page with this new page
    for (i = 0; i < numBuffers; i++) {
	//buffer manager selects from the vector of hated pages first
	     if (bufDescr[i].hate > 0 && bufDescr[i].pin_count == 0 && bufDescr[i].love == 0) {
	     if(iReplace==-1) iReplace = i;
	     else if (bufDescr[iReplace].hate > bufDescr[i].hate)  iReplace = i;	    
	    }
	}

  if(iReplace==-1){// no hated ones exist
    // from the loved pages 
    for (i = 0; i < numBuffers; i++) {
        if (bufDescr[i].love > 0 && bufDescr[i].pin_count == 0) {
	      if(iReplace==-1) iReplace = i;
	      else if (bufDescr[iReplace].love < bufDescr[i].love)  iReplace = i;	    
	    }
	}  
   } 
     
    if(iReplace==-1)//no frame found to replace, buffer is full
	      return 	MINIBASE_FIRST_ERROR( BUFMGR, BUFFERFULL);


    // iReplace has a valid frameid
    status = MINIBASE_DB->write_page(bufDescr[iReplace].pageNumber, &bufPool[iReplace]);// write old page onto disk
    if (status != OK)    return MINIBASE_CHAIN_ERROR(BUFMGR, status);

    status = MINIBASE_DB->read_page(PageId_in_a_DB, &bufPool[iReplace]);// read the new page into buffer pool
    if (status != OK)     return MINIBASE_CHAIN_ERROR(BUFMGR, status);

    //get pair in hashtbale first
    PageId curpageid = bufDescr[iReplace].pageNumber;
    entry = hashfunction(curpageid);


    //delete old entry in hashtable
    vector<hashpair>::iterator it2;
    for(it2 = hashtable[entry].begin(); it2 != hashtable[entry].end();){

        if(it2->frameNumber == iReplace)
            it2 = hashtable[entry].erase(it2);       
        else   
            it2++;
    }

    bufDescr[iReplace].pageNumber = PageId_in_a_DB;
    bufDescr[iReplace].pin_count = 1;
    bufDescr[iReplace].dirtybit = false;
    bufDescr[iReplace].love = 0;
    bufDescr[iReplace].hate = 0;
    // Point the page at the location in the buffer pool
    page = &bufPool[iReplace];


    // add new entry to hashtable
    struct hashpair k;

    entry = hashfunction(PageId_in_a_DB);

    k.pageNumber = PageId_in_a_DB;
    k.frameNumber = iReplace; 
    hashtable[entry].push_back(k);

    return OK;
}//end pinPage 

//*************************************************************
//** This is the implementation of unpinPage
//************************************************************
Status BufMgr::unpinPage(PageId page_num, int dirty=FALSE, int hate = FALSE){
//cout<<"unpinning page"<<page_num<<endl;
    unsigned int i;
    for (i = 0; i < numBuffers; i++) {
        if( bufDescr[i].pageNumber == page_num)  break;  //found the frame in buffer pool   
  }

    if(i==numBuffers) return MINIBASE_FIRST_ERROR( BUFMGR, BUFFERPAGENOTFOUND);

    if (bufDescr[i].pin_count == 0)
            return MINIBASE_FIRST_ERROR( BUFMGR, BUFFERPAGENOTPINNED);

    bufDescr[i].pin_count--;
    if (dirty != FALSE) flushPage(page_num);


    if(hate==FALSE){//page is loved; increment page's love. To ensure LRU policy, increment loved page's love
            bufDescr[i].love++;
            for(i = 0; i < numBuffers; i++)
                if (bufDescr[i].pageNumber != INVALID_PAGE)
                    if (bufDescr[i].love > 0) bufDescr[i].love++;
      }
    else{//page is hated; increment page's hate. To ensure MRU policy, increment hated page's hate
            bufDescr[i].hate++;
            for(i = 0; i < numBuffers; i++)
                if (bufDescr[i].pageNumber != INVALID_PAGE)
                    if (bufDescr[i].hate > 0) bufDescr[i].hate++; 
      }
  
  return OK;
}

//*************************************************************
//** This is the implementation of newPage
//************************************************************
Status BufMgr::newPage(PageId& firstPageId, Page*& firstpage, int howmany) {
  Status status, status_DB;
  status = MINIBASE_DB->allocate_page(firstPageId, howmany);
  if(status !=OK) return MINIBASE_CHAIN_ERROR(BUFMGR, status);

  status = pinPage(firstPageId, firstpage, TRUE);
  if(status !=OK){
        status_DB = MINIBASE_DB->deallocate_page(firstPageId, howmany);
        if (status_DB != OK) return MINIBASE_CHAIN_ERROR(BUFMGR, status_DB);

      	return MINIBASE_CHAIN_ERROR(BUFMGR, status);
      	}

  return OK;
}

//*************************************************************
//** This is the implementation of freePage
//************************************************************
Status BufMgr::freePage(PageId globalPageId){
    Status status;
    int entry = hashfunction(globalPageId), frameNumber=-1;


    vector<hashpair>::iterator it;
    
    for( it = hashtable[entry].begin(); it != hashtable[entry].end(); it++){

          if(it->pageNumber == globalPageId){ 
              frameNumber = it->frameNumber;
            } 
    }
    if(frameNumber==-1)
        return MINIBASE_FIRST_ERROR( BUFMGR, BUFFERPAGENOTFOUND);
    
    if (bufDescr[frameNumber].pin_count != 0)  return MINIBASE_FIRST_ERROR(BUFMGR, BUFFERPAGEPINNED);

    status = MINIBASE_DB->deallocate_page(globalPageId);
    if (status != OK) return MINIBASE_CHAIN_ERROR(BUFMGR, status);

  return OK;
}

//*************************************************************
//** This is the implementation of flushPage
//************************************************************
Status BufMgr::flushPage(PageId pageid) {
    Status status;
    int entry = hashfunction(pageid), frameNumber=-1;


    vector<hashpair>::iterator it;
    
    for( it = hashtable[entry].begin(); it != hashtable[entry].end(); it++){

          if(it->pageNumber == pageid){ 
              frameNumber = it->frameNumber;
            } 
    }
    if(frameNumber==-1)
        return MINIBASE_FIRST_ERROR( BUFMGR, BUFFERPAGENOTFOUND);

    // write the page onto disk
    status = MINIBASE_DB->write_page(pageid, &bufPool[frameNumber]);
    if (status != OK)
        return MINIBASE_CHAIN_ERROR(BUFMGR, status);

    bufDescr[frameNumber].dirtybit = false;

  return OK;
}
    
//*************************************************************
//** This is the implementation of flushAllPages
//************************************************************
Status BufMgr::flushAllPages(){
    unsigned int i;
    for (i = 0; i < numBuffers; i++) 
	      if(bufDescr[i].dirtybit == true){
           flushPage(bufDescr[i].pageNumber);
	    }

  return OK;
}


/*** Methods for compatibility with project 1 ***/
//*************************************************************
//** This is the implementation of pinPage
//************************************************************
Status BufMgr::pinPage(PageId PageId_in_a_DB, Page*& page, int emptyPage, const char *filename){
  return pinPage(PageId_in_a_DB, page, emptyPage);
}

//*************************************************************
//** This is the implementation of unpinPage
//************************************************************
Status BufMgr::unpinPage(PageId globalPageId_in_a_DB, int dirty, const char *filename){
  return unpinPage(globalPageId_in_a_DB, dirty);
}

//*************************************************************
//** This is the implementation of getNumUnpinnedBuffers
//************************************************************
unsigned int BufMgr::getNumUnpinnedBuffers(){
    unsigned int i, numOfBuffers = 0;
    for (i = 0; i < numBuffers; i++) {
        if ( bufDescr[i].pin_count == 0 )  numOfBuffers++;
        }

    return numOfBuffers;
}
