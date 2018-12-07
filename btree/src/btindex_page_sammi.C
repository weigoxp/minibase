/*
 * btindex_page.C - implementation of class BTIndexPage
 *
 * Johannes Gehrke & Gideon Glass  951016  CS564  UW-Madison
 * Edited by Young-K. Suh (yksuh@cs.arizona.edu) 03/27/14 CS560 Database Systems Implementation 
 */

#include "btindex_page.h"

// Define your Error Messge here
const char* BTIndexErrorMsgs[] = {
  //Possbile error messages,
  //OK,
  //Record Insertion Failure,
};

static error_string_table btree_table(BTINDEXPAGE, BTIndexErrorMsgs);

Status BTIndexPage::insertKey (const void *key,
                               AttrType key_type,
                               PageId pageNo,
                               RID& rid)
{
    Status status; 
    KeyDataEntry key_data_entry;
    Datatype data;
    data.pageNo = pageNo;
    int entryLength;

    make_entry(&key_data_entry, key_type, key, INDEX, data, &entryLength);  // header file :bt.h, definition file: key.C

    status = SortedPage::insertRecord (key_type, (char *) &key_data_entry, entryLength, rid);
    if (status != OK)    return MINIBASE_CHAIN_ERROR(BTINDEXPAGE, status);
  return OK;
} 

Status BTIndexPage::deleteKey (const void *key, AttrType key_type, RID& curRid)
{
    Status status;
    RID currentRid;
    PageId currentPageId;
    Keytype currentKey;
    int comparedValue;

    status = get_first(currentRid, &currentKey, currentPageId);
    if ( status != OK )   return status;    

    comparedValue = keyCompare(key, &currentKey, key_type);
    while(comparedValue>0 && status==OK)
	{
	status = get_next(currentRid, &currentKey, currentPageId);
	comparedValue = keyCompare(key, &currentKey, key_type);
	}

    if(status!=OK) return RECNOTFOUND;
	
    curRid = currentRid;

    status = deleteRecord(curRid);

  return OK;
}

Status BTIndexPage::get_page_no(const void *key,
                                AttrType key_type,
                                PageId & pageNo)
{
    Status status;
    RID currentRid;
    PageId currentPageId;
    Keytype currentKey;
    int comparedValue;
    
    pageNo = INVALID_PAGE;

    status = get_first(currentRid, &currentKey, currentPageId);
    if ( status != OK )   return status;    

    comparedValue = keyCompare(key, &currentKey, key_type);
    while(comparedValue>=0 && status==OK)
	{
	status = get_next(currentRid, &currentKey, currentPageId);
	comparedValue = keyCompare(key, &currentKey, key_type);
	}

    if(status!=OK) return RECNOTFOUND;
	
    pageNo = currentPageId;
/*
    PageId maxPageNo = INVALID_PAGE;
    RID currentRID;
    PageId tempPageNo;
    Keytype tempKey;

    Status status = get_first(currentRID, &tempKey, tempPageNo);

    while ( keyCompare(key, &tempKey, key_type) >= 0 && status == OK ) {
        maxPageNo = tempPageNo;
        status = get_next(currentRID, &tempKey, tempPageNo);
    }

    if ( maxPageNo == INVALID_PAGE && status == OK ) {
        pageNo = tempPageNo;
    } else {
        pageNo = maxPageNo;
    }
char c;
cout<<"\nBTIndexPage::get_page_no \n pageNo="<<pageNo<<endl;
cin>>c;
  return OK;
*/
}

    
Status BTIndexPage::get_first(RID& rid,
                              void *key,
                              PageId & pageNo)
{
    Status status;
    int entryLength;
    KeyDataEntry entry;
    Datatype data;

    status = HFPage::firstRecord(rid);
    if (status != OK)   return RECNOTFOUND;

    status = HFPage::getRecord(rid, (char *) &entry, entryLength);
    if (status != OK)   return MINIBASE_CHAIN_ERROR(BTLEAFPAGE, status);

    get_key_data( key, &data, &entry, entryLength, INDEX);
    pageNo = data.pageNo;
  return OK;
}

Status BTIndexPage::get_next(RID& rid, void *key, PageId & pageNo)
{
    Status status;
    int entryLength;
    KeyDataEntry entry;
    Datatype data;

    status = HFPage::nextRecord(rid,rid);
    if (status != OK)   return NOMORERECS;

    status = HFPage::getRecord(rid, (char *) &entry, entryLength);
    if (status != OK)   return MINIBASE_CHAIN_ERROR(BTLEAFPAGE, status);

    get_key_data( key, &data, &entry, entryLength, INDEX);
    pageNo = data.pageNo;

  return OK;
}
