/*
 * btfile.C - function members of class BTreeFile 
 * 
 * Johannes Gehrke & Gideon Glass  951022  CS564  UW-Madison
 * Edited by Young-K. Suh (yksuh@cs.arizona.edu) 03/27/14 CS560 Database Systems Implementation 
 */

#include "minirel.h"
#include "buf.h"
#include "db.h"
#include "new_error.h"
#include "btfile.h"
#include "btreefilescan.h"

#include <algorithm>

// Define your error message here
const char* BtreeErrorMsgs[] = {
  // Possible error messages
  // _OK
  // CANT_FIND_HEADER
  // CANT_PIN_HEADER,
  // CANT_ALLOC_HEADER
  // CANT_ADD_FILE_ENTRY
  // CANT_UNPIN_HEADER
  // CANT_PIN_PAGE
  // CANT_UNPIN_PAGE
  // INVALID_SCAN
  // SORTED_PAGE_DELETE_CURRENT_FAILED
  // CANT_DELETE_FILE_ENTRY
  // CANT_FREE_PAGE,
  // CANT_DELETE_SUBTREE,
  // KEY_TOO_LONG
  // INSERT_FAILED
  // COULD_NOT_CREATE_ROOT
  // DELETE_DATAENTRY_FAILED
  // DATA_ENTRY_NOT_FOUND
  // CANT_GET_PAGE_NO
  // CANT_ALLOCATE_NEW_PAGE
  // CANT_SPLIT_LEAF_PAGE
  // CANT_SPLIT_INDEX_PAGE
};

static error_string_table btree_table( BTREE, BtreeErrorMsgs);


/* Constructor for returnstatus for fetching a file
 * If does not exist, then exit otherwise initialize
 * Headerpage for operations
 */
BTreeFile::BTreeFile (Status& returnStatus, const char *filename)
{   
    returnStatus = MINIBASE_DB->get_file_entry(filename,headerPageId);
    if(returnStatus!=OK)  return;

    returnStatus = MINIBASE_BM->pinPage(headerPageId, (Page*&) headerPage, TRUE);
    this->fileName = filename;
}

/* See if a particular index exists with some key
 * if not, add the index in file. Otherwise pin to buffer
 */
BTreeFile::BTreeFile (Status& returnStatus, const char *filename, 
                      const AttrType keytype,
                      const int keysize)
{
    Status status = MINIBASE_DB->get_file_entry(filename,headerPageId);
    this->fileName = filename;
    if(status!=OK) //file doesn't exist; create the page
	{
		returnStatus = MINIBASE_BM->newPage(headerPageId, (Page*&) headerPage,1);
		headerPage->keyType = keytype;
		headerPage->keySize = keysize;
		headerPage->pageId = -1;
		headerPage->numLevels = 0;
		MINIBASE_DB->add_file_entry(fileName.c_str(), headerPageId);
		return;
	}
	else
	{
		returnStatus = MINIBASE_BM->pinPage(headerPageId, (Page*&) headerPage, 0,filename);
	}
}


/* Unpin the index file after turning off or work done or replace
 */
BTreeFile::~BTreeFile ()
{
  MINIBASE_BM->unpinPage(headerPageId,true,fileName.c_str());
}

Status BTreeFile::destroyFile ()
{
  return MINIBASE_DB->delete_file_entry(fileName.c_str());
}

/* inserting an index key
 */

Status BTreeFile::insert(const void *key, const RID rid) {
	if(headerPage->keyType == attrInteger)
	{
		int *val = (int *)key;

		// only insert if value does not exist
		// we are not allowing duplicates in index nodes
		if ((std::find(intValues.begin(), intValues.end(), *val) != intValues.end()) == false)
		{
			intValues.push_back(*val);
			int len;
			char *record = create_key_data_record(key,rid,len);
			intRecords[*val]=record;
		}  
		else
		{
			return OK;
		}
	}
	else if(headerPage->keyType == attrString)
	{
		char *k = (char *)key;
		string keyVal = k;
		stringValues.push_back(keyVal);
		int len;
		char *record = create_key_data_record(key,rid,len);
		stringRecords[k]=record;
	}
	
	// in case nothing is in the DB,
	// we start with a root and enter the index entry
	// and create the index for first page
	if(headerPage->pageId ==-1)
	{
		PageId  rootId;
		Page *pg;
		MINIBASE_BM->newPage(rootId,pg,1);
		BTLeafPage *root = (BTLeafPage *)pg;
		root->init(rootId);
		headerPage->pageId = rootId;
		MINIBASE_BM->unpinPage(rootId,true,fileName.c_str());
		KeyDataEntry *child=NULL;
		PageId splitPageId;
		insert(rootId,key,rid,child,splitPageId);
	}
	// otherwise we see if the insert
	// causes an overflow or not
	else
	{
		PageId pgId = headerPage->pageId;
		KeyDataEntry *entry=NULL;
		PageId splitPageId=-1;
		insert(pgId,key,rid,entry,splitPageId);

		// in case it does cause a split
		// we need to create a new page for accomodating half of the 
		// contents of the current page
		// and also create a parent for the new two
		// child pages.
		// if we get a new root, we will unpin the old root
		// and pin the new one
		if(entry!=NULL && splitPageId==headerPage->pageId)
		{
			Page *rootPage;
			PageId rootId;
			MINIBASE_BM->newPage(rootId,rootPage,1);
			BTIndexPage *root = (BTIndexPage *)rootPage;
			root->init(rootId);
			RID temp;

			root->setLeftLink(headerPage->pageId);
			if(headerPage->keyType==attrInteger)
			{
				root->insertKey(&entry->key.intkey,headerPage->keyType,splitPageId,temp);
			}
			else if(headerPage->keyType==attrString)
			{
				root->insertKey(entry->key.charkey,headerPage->keyType,splitPageId,temp);
			}
			headerPage->pageId = rootId;
			MINIBASE_BM->unpinPage(rootId,true,fileName.c_str());
		}
		// we did not get the root to be split
		// thus we pin the page, insert key and unpin the page
		else if(entry!=NULL && splitPageId!=headerPage->pageId)
		{
			Page *pg;
			RID rid;
			MINIBASE_BM->pinPage(headerPage->pageId,pg,0,fileName.c_str());
			BTIndexPage *root = (BTIndexPage *)pg;
			root->insertKey(&entry->key.intkey,headerPage->keyType,entry->data.pageNo,rid);
			MINIBASE_BM->unpinPage(headerPage->pageId,true,fileName.c_str());
		}
	}
	return OK;
}


void BTreeFile::insert(PageId &pageId, const void *key, RID rid, KeyDataEntry *&childEntry, PageId& splitPageId) {
	//PageId pageNum;
//	void *k;

//	RID r;
	//int prevCompare = 0, compare = 0;
	SortedPage *pg;
	Page *currPage;
	MINIBASE_BM->pinPage(pageId,currPage,0,fileName.c_str());
	pg = (SortedPage *)currPage;
	short type = pg->get_type();
	
	// if the page type is index
	// we'll check if the entry can be occupied
	// if can, we'll insert key,
	// otherwise split occurs. 
	if(type == INDEX)
	{
		BTIndexPage *page = (BTIndexPage *)pg;
		PageId insertionPageId ;
		page->get_page_no( key, headerPage->keyType, insertionPageId);
		//KeyDataEntry *newEntry = new KeyDataEntry();
		MINIBASE_BM->unpinPage(pageId,true,fileName.c_str());
		insert(insertionPageId,key,rid,childEntry,splitPageId);
		if(childEntry==NULL)
		{
			return;
		}
		if(childEntry != NULL)
		{
			if(page->available_space()>sizeof(KeyDataEntry))
			{
				int entryLen = get_key_data_length(key,headerPage->keyType,INDEX);
				void *k;
				if(headerPage->keyType == attrInteger)
				{
					k = new int[1];
				}
				else{
					k = new char[MAX_KEY_SIZE1];
				}
				Datatype *dt = new Datatype();
				RID temp;
				get_key_data(k,dt,childEntry,entryLen,INDEX);
				Status st = page->insertKey(k,headerPage->keyType,dt->pageNo,temp);
				if(st!=OK){}
				childEntry = NULL;
				return;
			}
			// else we split and put half of index to right sibling
			else
			{
				BTIndexPage *rightSibling;
				PageId rightId;
				Page *rightPg;
				MINIBASE_BM->newPage(rightId,rightPg,1);
				rightSibling = (BTIndexPage *)rightSibling;
				rightSibling->init(rightId);
				splitIndexPage(page,rightSibling);
				void *k;
				RID tempRID;
				PageId rightSmallestId;
				rightSibling->get_first(tempRID,k,rightSmallestId);
				childEntry = new KeyDataEntry();
				if(headerPage->keyType == attrInteger)
				{
					memcpy(&(childEntry->key.intkey),k,sizeof(int));
				}
				else
				{
					memcpy((childEntry->key.charkey),k,get_key_length(k,headerPage->keyType));
				}

				PageId rightPageId = rightSibling->page_no();
				memcpy(&childEntry->data.pageNo,&rightPageId,sizeof(PageId));
				rightSibling->deleteRecord(tempRID);
				
				// if we get eventual root split
				// we create a new page for the root
				// and "push up" a mid key from the right child,
				// and set left link
				// finally we free the old root page 
				if(page->page_no() == headerPage->pageId)
				{
					Page *newRoot;
					PageId newId;
					MINIBASE_BM->newPage(newId,newRoot,1);
					BTIndexPage *root;
					root = (BTIndexPage *)newRoot;
					root->init(newId);
					RID temp;
					if(headerPage->keyType == attrInteger)
					{
						root->insertKey(&childEntry->key.intkey,headerPage->keyType,childEntry->data.pageNo,temp);
					}
					else if(headerPage->keyType == attrString)
					{
						root->insertKey(&childEntry->key.charkey,headerPage->keyType,childEntry->data.pageNo,temp);
					}
					root->setLeftLink(page->page_no());
					headerPage->pageId = root->page_no();
					MINIBASE_BM->unpinPage(newId,true,fileName.c_str());
				}
				MINIBASE_BM->unpinPage(pageId,true,fileName.c_str());
				return;
			}
		}
	}
	// a leaf node entry
	// we'll insert actual record 
	else 
	{
		int entryLen = get_key_data_length(key,headerPage->keyType,LEAF);
		BTLeafPage *leaf = (BTLeafPage *)pg;

		// we have space available, so will only insert the record
		if(leaf->available_space() > entryLen)
		{
			RID tempId;
			leaf->insertRec(key,headerPage->keyType,rid,tempId);
			childEntry = NULL;
			MINIBASE_BM->unpinPage(leaf->page_no(),true,fileName.c_str());
			return;
		}
		// if leaf node is full, we must split
		// and "copy up" first entry of right sibling to parent node
		else 
		{
			BTLeafPage *rightSibling;
			Page *rightPage;
			PageId rightId;
			MINIBASE_BM->newPage(rightId,rightPage,1);
			rightSibling = (BTLeafPage *)rightPage;
			rightSibling->init(rightId);
			splitLeafPage(leaf,rightSibling);
			childEntry = new KeyDataEntry();
			void *rightKey;
			if(headerPage->keyType==attrInteger)
			{
				rightKey = new int[1];
			}
			else if(headerPage->keyType==attrString)
			{
				rightKey = new char[MAX_KEY_SIZE1];
			}
			RID rightRID,temp;
			splitPageId = leaf->page_no();
			rightSibling->get_first(temp,rightKey,rightRID); // firt key of right sibling to be "copied up"
			if(headerPage->keyType == attrInteger)
			{
				memcpy(&childEntry->key.intkey, rightKey, sizeof(int));
			}
			else
			{
				memcpy(childEntry->key.charkey,rightKey,get_key_length(rightKey,headerPage->keyType));
			}
			PageId tempPageId = leaf->page_no();
			memcpy(&childEntry->data.rid, &tempPageId, sizeof(RID));

			// has siblings already
			// so we get the next sibling page of the current right sibling
			// setting pointer from left sibling to right sibling
			if(leaf->getNextPage()!=-1) 
			{
				rightSibling->setNextPage(leaf->getNextPage());
			}
			leaf->setNextPage(rightSibling->page_no());
			rightSibling->setPrevPage(leaf->page_no());
			if(keyCompare(key,rightKey,headerPage->keyType) < 0)
			{
				Status st = leaf->insertRec(key,headerPage->keyType,rid,temp);
				if(st!=OK)
				{
					cout<<"Error inserting"<<endl;
				}
			}
			else
			{
				Status st = rightSibling->insertRec(key,headerPage->keyType,rid,temp);
				if(st!=OK)
				cout<<"Error inserting"<<endl;
			}
			MINIBASE_BM->unpinPage(rightId,true,fileName.c_str());
		}
		return;
	}
}

/* removing a record from the btree
 */
Status BTreeFile::Delete(const void *key, const RID rid) {
  // put your code here

	if(headerPage->keyType == attrInteger)
	{
		int *val = (int *)key;
		intValues.erase(std::remove(intValues.begin(),intValues.end(),*val),intValues.end());
	} 
	else if(headerPage->keyType == attrString)
	{
		char *val = (char *)key;
		string str = val;
	}
	
	PageId rootId = headerPage->pageId;
	Page *page;
	MINIBASE_BM->pinPage(rootId,page,0,fileName.c_str());
	PageId leftmostLeaf;
	Page *leftmostLeafPage;
	SortedPage *pointer;
	pointer = (SortedPage *)page;
	
	// to keep the leaf node entries sorted, we will create new data
	// record at the deleted space and shift right side records towards left
	// to fill out the spacein the middle
	if(pointer->get_type()==LEAF)
	{
		int len;
		char *rec = create_key_data_record(key,rid,len);
		void *currKey;
		if(headerPage->keyType == attrInteger)
		{
			currKey = new int[1];
		}
		else
		{
			currKey = new char[MAX_KEY_SIZE1];
		}
		RID currRID,temp;
		BTLeafPage *leafPage = (BTLeafPage *)page;
		Status status = leafPage->get_first(temp,currKey,currRID);
		while(status == OK)
		{
			char *currRecord = create_key_data_record(currKey,currRID,len);
			if(strcmp(currRecord,rec)==0)
			{
				Status status = leafPage->deleteRecord(temp);
				MINIBASE_BM->unpinPage(rootId,true,fileName.c_str());
				return status;
			}
			status = leafPage->get_next(temp,currKey,currRID);
		}
		MINIBASE_BM->unpinPage(rootId,true,fileName.c_str());
		return DONE;
	}
	// if the deleted one causes index deletion
	// we do similar and shift the leaf nodes
	else
	{
		BTIndexPage *currPage = (BTIndexPage *)page;
		int len;
		void *currentKey;
		RID currRID,temp;

		if(headerPage->keyType == attrInteger)
		{
			currentKey = new int[1];
		} 
		else
		{
			currentKey = new char[MAX_KEY_SIZE1];
		}
		while(currPage->getLeftLink()!=-1)
		{
			leftmostLeaf = currPage->getLeftLink();
			MINIBASE_BM->pinPage(leftmostLeaf,leftmostLeafPage,0,fileName.c_str());
			MINIBASE_BM->unpinPage(currPage->page_no(),true,fileName.c_str());
			currPage = (BTIndexPage *)leftmostLeafPage;
		}
		BTLeafPage *leaf = (BTLeafPage *)leftmostLeafPage;
		create_key_data_record(key,rid,len);
		Status status = leaf->get_first(temp,currentKey,currRID);
		while(true)
		{
			if(status!=OK)
			{
				PageId currPageId = leaf->page_no();
				PageId nextLeafId = leaf->getNextPage();
				if(nextLeafId==-1)
				{
					break;
				}
				MINIBASE_BM->unpinPage(currPageId,true,fileName.c_str());
				MINIBASE_BM->pinPage(nextLeafId,leftmostLeafPage,0,fileName.c_str());
				leaf = (BTLeafPage *)leftmostLeafPage;
				status = leaf->get_first(temp,currentKey,currRID);
				continue;
			}
			
			 create_key_data_record(currentKey,currRID,len);

			if(keyCompare(currentKey,key,headerPage->keyType)==0)
			{
				Status deleteStatus = leaf->deleteRecord(temp);
				MINIBASE_BM->unpinPage(leaf->page_no(),true,fileName.c_str());
				return deleteStatus;
			}
			status = leaf->get_next(temp,currentKey,currRID);
		}
		MINIBASE_BM->unpinPage(leaf->page_no(),true,fileName.c_str());
		return DONE;
	}
	return DONE;
}

 
/* a scan within a range is being issued
 */
IndexFileScan *BTreeFile::new_scan(const void *lo_key, const void *hi_key) {
  // put your codd
	
	BTreeFileScan *scan = new BTreeFileScan(this,lo_key,hi_key,headerPage->keyType);
	return scan;
}

int BTreeFile::keysize(){
  // put your code here
  return headerPage->keySize; // returns the key lenght
}


/* This splits an index node page into half creates two child nodes and 
 * tries to "push up" if non-leaf from right sibling
 * and insert the record in leaf
 */
Status BTreeFile::splitIndexPage(BTIndexPage *page, BTIndexPage *newPage)
{
	Status status;
	int i=0, recLen, firstCount,secondCount, recordCount = page->numberOfRecords();
	RID rid, recId;
	vector<RID> deleted;
	PageId pageId;
	void *key;

	firstCount = recordCount/2 + recordCount%2;
	secondCount = recordCount/2;

	//go to the middle of page
	page->get_first(rid,key,pageId);
	while(i<firstCount)
	{
		page->get_next(rid,key,pageId);
		i++;
		delete(key);
	}

	i=0;
   	switch(headerPage->keyType){
		case attrInteger: key = (void *)(new int[1]); break;
		case attrString: key = new char[MAX_KEY_SIZE1]; break;
		default: break;
	}
	while(i<secondCount)
	{
		status = page->get_next(rid,key,pageId);
		if(status!=OK)	return status;
		deleted.push_back(rid);
		char *rec = create_key_index_record(key,pageId,recLen);
		newPage->insertRecord(headerPage->keyType,rec,recLen,recId);
		i++;
		delete(key);
	}

	//delete records from page
	for(int i=deleted.size();i>=0;i--)
	{
		Status status = page->deleteRecord(deleted[i]);
		if(status!=OK) return status;
	}
	return OK;
}


/* This splits a leaf node page into half creates two child nodes 
 * sets pointer from left sibling to right sibling and 
 * does copy up from right sibling
 * and insert the record in leaf
 */

Status BTreeFile::splitLeafPage(BTLeafPage *leafPage, BTLeafPage *newLeafPage) 
{ 
	Status status;
	int i=0, halfRecords, secondHalfRecords, numRecords = leafPage->numberOfRecords();
	RID rid, dataRID, tempRid;
	vector<RID> deletedRID;
	void *key;

	secondHalfRecords = numRecords/2;
	halfRecords = numRecords - secondHalfRecords;

   	switch(headerPage->keyType){
		case attrInteger: key = (void *)(new int[1]); break;
		case attrString: key = new char[MAX_KEY_SIZE1]; break;
		default: break;
	}
	
	status = leafPage->get_first(rid,key,dataRID);
	while(i < halfRecords)
	{
		status = leafPage->get_next(rid,key,dataRID);
		i++;
	}

	i=0;
	while(i<secondHalfRecords)
	{
		status = newLeafPage->insertRec(key,headerPage->keyType,dataRID,tempRid);
		if(status!=OK)	return status;

		deletedRID.push_back(rid);
		status = leafPage->get_next(rid,key,dataRID);
		i++;
	}

	//delete records from leafPage
	for(i = deletedRID.size()-1; i>=0; i--) 
	{
		status = leafPage->deleteRecord(deletedRID[i]);
		if(status!=OK)	return status;
	}

	return OK;
}

/* For actual record entry with key
 */
char* BTreeFile::create_key_data_record(const void *key, RID dataRId,int& recLen)
{
	KeyDataEntry record;
	Datatype data;
	data.rid=dataRId;
	int entry_len;
	make_entry(&record, headerPage->keyType, key, LEAF, data, &entry_len);
	return (char*) &record;
}

/* For index to page entry with key
 */
char* BTreeFile::create_key_index_record(const void *key, PageId pageNum, int &recLen)
{
	KeyDataEntry record;
	Datatype data;
	data.pageNo = pageNum;
	int entry_len;
	make_entry(&record, headerPage->keyType, key, INDEX, data, &entry_len);
	return (char*) &record;
}
