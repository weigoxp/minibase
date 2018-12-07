/*****************************************************************************/
/*************** Implementation of the Buffer Manager Layer ******************/
/*****************************************************************************/

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

PageReplacer::PageReplacer() {}

void PageReplacer::addToMruStack(PageId pageNum) {
	MruStack.push(pageNum);
}

void PageReplacer::addToLruQueue(PageId pageNum) {
	LruQueue.push(pageNum);
}

PageId PageReplacer::getVictim() {
	// trying to evict, but already empty, invalid
	if(MruStack.empty() && LruQueue.empty())
	{
		return -1;
	}

	if(!MruStack.empty())
	{
		PageId pageNum = MruStack.top();
		if(LruQueue.front() == pageNum)
		{
			MruStack.pop();
			pageNum = MruStack.top();
		}
		MruStack.pop();
		return pageNum;
	}
	if(!LruQueue.empty())
	{
		PageId pageNum = LruQueue.front();
		LruQueue.pop();
		return pageNum;
	}
	return -1; // failsafe if anyhow the stack or queue gets corrupted
}

// Create a static "error_string_table" object and register the error messages
// with minibase system
static error_string_table bufTable(BUFMGR,bufErrMsgs);

//*************************************************************
//** This is the implementation of BufMgr
//************************************************************

BufMgr::BufMgr (int numbuf, Replacer *replacer) {
  // put your code here
	numBuffers = numbuf;

	hashTable = new HashEntry*[HTSIZE];
	bufPool = (Page *)malloc(numbuf * sizeof(Page));
	memset(bufPool, 0, numbuf * sizeof(Page));
	this->descriptors = new Descriptor[numbuf];
	for(int i=0;i<numbuf;i++)
	{
		descriptors[i].pageNum = -1;
		descriptors[i].pin_count = 0;
		descriptors[i].dirty = false;
	}

	for(int i=0;i<HTSIZE;i++)
	{
		hashTable[i]=NULL;
	}

	this->replacer = new PageReplacer();
}

//*************************************************************
//** This is the implementation of ~BufMgr
//************************************************************
BufMgr::~BufMgr(){
  // put your code here
}

//*************************************************************
//** This is the implementation of pinPage
//************************************************************
/* we are pinning page so that operations on a page can be done and no other
 * application can evict this page from buffer pool
 * however, as we may face overflow, pinning a page may require
 * to find a victim page and flush that first
 */
Status BufMgr::pinPage(PageId PageId_in_a_DB, Page*& page, int emptyPage) {
  // put your code here
	for(unsigned int i=0; i < numBuffers; i++)
	{
		if(descriptors[i].pageNum == PageId_in_a_DB)
		{
			descriptors[i].pin_count+=1;
			int frameIndex = hash(PageId_in_a_DB);
			HashEntry *entry = hashTable[frameIndex];
			while(entry!= NULL)
			{
				if(entry->pageNum == PageId_in_a_DB)
				{
					break;
				}
				entry = entry->next;
			}
			if(entry==NULL)
			{
				return MINIBASE_FIRST_ERROR(BUFMGR,BUFFERPAGENOTFOUND);
			}

			page = bufPool + entry->frameNum;

			return OK;
		}
	}

	for(unsigned int i=0; i < numBuffers; i++)
    {
		if(descriptors[i].pageNum==-1)
		{
			int frameIndex = hash(PageId_in_a_DB);
			HashEntry *entry = new HashEntry();
			entry->pageNum = PageId_in_a_DB;
			entry->frameNum = i;
			HashEntry *current = hashTable[frameIndex];
			entry->next = NULL;

			if(current!=NULL)
			{
				while (current->next!=NULL)
				{
					current=current->next;
				}
				current->next = entry;
			}
			else
			{
				hashTable[frameIndex] = entry;
			}

			page = bufPool + i;
			if(emptyPage!=TRUE)
			{
				Status readStatus = MINIBASE_DB->read_page(PageId_in_a_DB,page);
				if(readStatus!=OK)
				{
					return MINIBASE_CHAIN_ERROR(BUFMGR,readStatus);
				}

				memcpy(bufPool + i,page,sizeof(Page));
			}
			descriptors[i].pin_count += 1;
			descriptors[i].pageNum = PageId_in_a_DB;
			return OK;
		}
    }

	PageId victim = replacer->getVictim();
	int frameIndex = hash(victim);
	HashEntry *entry = hashTable[frameIndex];
	while(entry)
	{
		if(entry->pageNum == victim)
		{
			break;
		}

		entry = entry->next;
	}

	Status flushStatus =  flushPage(victim);

	descriptors[entry->frameNum].pageNum= PageId_in_a_DB;
	page = bufPool + entry->frameNum;
	Status readStatus = MINIBASE_DB->read_page(PageId_in_a_DB,page);

	descriptors[entry->frameNum].pin_count += 1;
	int victimFrame = entry->frameNum;

	if(readStatus==OK && emptyPage!=true)
	{
		memcpy(bufPool + entry->frameNum, page, sizeof(Page));
	}

	if(flushStatus!=OK)
	{
		return MINIBASE_FIRST_ERROR(BUFMGR,BUFMGRMEMORYERROR);
	}

	int newFrameIndex = hash(PageId_in_a_DB);
	HashEntry *newEntry = hashTable[newFrameIndex];

	if(newEntry==NULL)
	{
		newEntry = new HashEntry();
		newEntry->next = NULL;
		newEntry->frameNum =  victimFrame;
		newEntry->pageNum = PageId_in_a_DB;
	}
	else
	{
		while(newEntry->next)
		{
			newEntry= newEntry->next;
		}

		newEntry->next = new HashEntry();
		newEntry->next->next = NULL;
		newEntry->next->pageNum = PageId_in_a_DB;
		newEntry->next->frameNum = victimFrame;
	}

  return OK;
}//end pinPage

//*************************************************************
//** This is the implementation of unpinPage
//************************************************************
Status BufMgr::unpinPage(PageId page_num, int dirty=FALSE, int hate = FALSE){
  // put your code here
	for(unsigned int i=0; i < numBuffers; i++)
	{
		if(descriptors[i].pageNum == page_num)
		{
			int frameIndex = hash(page_num);
			HashEntry *entry = hashTable[frameIndex];
			Page *page = bufPool + entry->frameNum;

			if(descriptors[i].pin_count == 0)
			{
				return MINIBASE_FIRST_ERROR(BUFMGR, BUFFERPAGENOTPINNED);
			}

			if(descriptors[i].pageNum == page_num)
			{
				descriptors[i].pin_count -= 1;
				descriptors[i].dirty = dirty;

				// based on love/hate we set either to MRU or LRU
				if(descriptors[i].pin_count == 0 && hate == TRUE)
				{
					replacer->addToMruStack(page_num);
				}
				else if(descriptors[i].pin_count == 0 && hate == FALSE)
				{
					replacer->addToLruQueue(page_num);
				}

				if(dirty==TRUE && descriptors[i].pin_count==0)
				{
					Status writeStatus = MINIBASE_DB->write_page(descriptors[i].pageNum, page);
					if(writeStatus != OK)
					{
						return MINIBASE_CHAIN_ERROR(BUFMGR, writeStatus);
					}
				}


				return OK;
			}
		}
	}
  return DONE;
}

//*************************************************************
//** This is the implementation of newPage
//************************************************************
Status BufMgr::newPage(PageId& firstPageId, Page*& firstpage, int howmany) {
  // put your code here
	for(unsigned int i=0; i < numBuffers; i++)
	{
		if(descriptors[i].pageNum == -1)
		{
			MINIBASE_DB->allocate_page(firstPageId, howmany);

			descriptors[i].pageNum = firstPageId;
			descriptors[i].pin_count = 1;
			descriptors[i].dirty = false;
			int frameIndex = hash(firstPageId);
			firstpage = bufPool + i;

			HashEntry *entry = new HashEntry();
			entry->pageNum = firstPageId;
			entry->frameNum = i;
			entry->next = NULL;

			HashEntry *current = hashTable[frameIndex];
			if(current!=NULL)
			{
				while(current->next!=NULL)
				{
					current=current->next;
				}

				current->next = entry;
			}
			else
			{
				hashTable[frameIndex] = entry;
			}

			memcpy(bufPool + i, firstpage, sizeof(Page));

			return OK;
		}
	}

	return MINIBASE_FIRST_ERROR(BUFMGR, BUFFERFULL);
}

//*************************************************************
//** This is the implementation of freePage
//************************************************************
Status BufMgr::freePage(PageId globalPageId){
  // put your code here
	for(unsigned int i=0; i < numBuffers; i++)
	{
		if(descriptors[i].pageNum == globalPageId && descriptors[i].pin_count != 0)
		{
			return MINIBASE_FIRST_ERROR(BUFMGR, BUFFERPAGEPINNED);
		}

		if(descriptors[i].pageNum == globalPageId)
		{
			descriptors[i].pageNum=-1;
			descriptors[i].pin_count = 0;
			Status deallocateStatus =  MINIBASE_DB->deallocate_page(globalPageId, 1);

			if(deallocateStatus != OK)
			{
				return MINIBASE_CHAIN_ERROR(BUFMGR, deallocateStatus);
			}

			int frameIndex = hash(globalPageId);

			HashEntry *entry = hashTable[frameIndex];

			while(entry)
			{
				if(entry->pageNum == globalPageId)
				{
					break;
				}

				entry = entry->next;
			}
			if(entry->next==NULL)
			{
				delete(entry);
			}
			else
			{
				HashEntry *next = entry->next;
				delete(entry);
				entry = next;
			}
		}
	}
  return OK;
}

//*************************************************************
//** This is the implementation of flushPage
//************************************************************
Status BufMgr::flushPage(PageId pageid) {
  // put your code here
	for(unsigned int i=0; i < numBuffers; i++)
	{
		if(descriptors[i].pageNum==pageid)
		{
			int frameIndex = hash(pageid);
			HashEntry *entry = hashTable[frameIndex];
			while(entry!=NULL)
			{
				if(entry->pageNum == pageid)
				{
					break;
				}

				entry = entry->next;
			}

			if(entry==NULL)
			{
				return MINIBASE_FIRST_ERROR(BUFMGR, HASHNOTFOUND);
			}

			int frameNum = entry->frameNum;
			Page *page = &bufPool[frameNum];
			Status writeStatus = MINIBASE_DB->write_page(pageid, page);

			if(writeStatus != OK)
			{
				return MINIBASE_CHAIN_ERROR(BUFMGR, writeStatus);
			}

			return OK;
		}
	}
  return MINIBASE_FIRST_ERROR(BUFMGR, BUFFERPAGENOTFOUND);
}

//*************************************************************
//** This is the implementation of flushAllPages
//************************************************************
Status BufMgr::flushAllPages(){
  //put your code here
	for(unsigned int i=0; i < numBuffers; i++)
	{
		if(descriptors[i].pageNum != -1)
		{
			Status status = flushPage(descriptors[i].pageNum);
			if(status != OK)
			{
				return MINIBASE_FIRST_ERROR(BUFMGR, INTERNALERROR);
			}
		}
	}
  return OK;
}


/*** Methods for compatibility with project 1 ***/
//*************************************************************
//** This is the implementation of pinPage
//************************************************************
Status BufMgr::pinPage(PageId PageId_in_a_DB, Page*& page, int emptyPage, const char *filename){
  //put your code here
  return pinPage(PageId_in_a_DB, page, emptyPage);
}

//*************************************************************
//** This is the implementation of unpinPage
//************************************************************
Status BufMgr::unpinPage(PageId globalPageId_in_a_DB, int dirty, const char *filename){
  //put your code here
  return unpinPage(globalPageId_in_a_DB, dirty, FALSE);
}

//*************************************************************
//** This is the implementation of getNumUnpinnedBuffers
//************************************************************
unsigned int BufMgr::getNumUnpinnedBuffers(){
  //put your code here
	unsigned int unpinned = 0;
	for(unsigned int i=0; i < numBuffers; i++)
	{
		if(descriptors[i].pin_count==0)
		{
			unpinned++;
		}
	}
	return unpinned;
}

//*************************************************************
//** This is the hash function
//************************************************************
int BufMgr::hash(int pageNum)
{
	return (2 * pageNum + 3) % HTSIZE;
}
