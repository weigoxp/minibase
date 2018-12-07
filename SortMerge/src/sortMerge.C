
#include <string.h>
#include <assert.h>
#include "sortMerge.h"

// Error Protocall:


enum ErrCodes {SORT_FAILED, HEAPFILE_FAILED};

static const char* ErrMsgs[] =  {
  "Error: Sort Failed.",
  "Error: HeapFile Failed."
  // maybe more ...
};
struct _rec {
	int	key;
	char	filler[4];
};
static error_string_table ErrTable( JOINS, ErrMsgs );

// sortMerge constructor
sortMerge::sortMerge(
    char*           filename1,      // Name of heapfile for relation R
    int             len_in1,        // # of columns in R.
    AttrType        in1[],          // Array containing field types of R.
    short           t1_str_sizes[], // Array containing size of columns in R
    int             join_col_in1,   // The join column of R 

    char*           filename2,      // Name of heapfile for relation S
    int             len_in2,        // # of columns in S.
    AttrType        in2[],          // Array containing field types of S.
    short           t2_str_sizes[], // Array containing size of columns in S
    int             join_col_in2,   // The join column of S

    char*           filename3,      // Name of heapfile for merged results
    int             amt_of_mem,     // Number of pages available
    TupleOrder      order,          // Sorting order: Ascending or Descending
    Status&         s               // Status of constructor
){

	Status status1, status2, status3;

	// get filenames for both sorted files.
	char* sortedfilename1 = "sortedfile1";
	char* sortedfilename2 = "sortedfile2";

	// create heapfiles container
	HeapFile *sortedfile1 = new HeapFile(sortedfilename1,status1);
	HeapFile *sortedfile2 = new HeapFile(sortedfilename2,status2);
	HeapFile *joinedfile = new HeapFile(filename3,status3);


	//sort files from filename 1 2 3 to ready heapfiles
	Sort(filename1, sortedfilename1, len_in1, in1,
						 t1_str_sizes, join_col_in1, order, amt_of_mem, status1);

	Sort(filename2, sortedfilename2, len_in2, in2,
						 t2_str_sizes, join_col_in2, order, amt_of_mem, status2);

	// open heapfile scan 
	Scan *scan1 = sortedfile1->openScan(status1);
	Scan *scan2 = sortedfile2->openScan(status2);

	//scan is already initilized.
	RID curRid1,curRid2;
	char* curRecPtr1 =new char[8];
	char* curRecPtr2 =new char[8];
	int curlen1,curlen2;

	scan1->getNext(curRid1, curRecPtr1, curlen1);
	scan2->getNext(curRid2, curRecPtr2, curlen2);
	 	// compare table

	Status scanstatus1 = OK;
	Status scanstatus2 = OK;

	RID dumb;
	while(scanstatus1==OK && scanstatus2 == OK){


		if(tupleCmp(curRecPtr1,curRecPtr2)>0){
			scanstatus2 =scan2->getNext(curRid2, curRecPtr2, curlen2);
			
		}
		
		else if (tupleCmp(curRecPtr1,curRecPtr2)<0){
			scanstatus1 = scan1->getNext(curRid1, curRecPtr1, curlen1);
	
		}

		// equal, time to merge. 
		else{

            while(tupleCmp(curRecPtr1,curRecPtr2)==0 && scanstatus1==OK ){
            //	printf("R is : %d", ((struct _rec *)curRecPtr1)->key);
				//printf("   S is : %d\n", ((struct _rec *) curRecPtr2)->key);
            	RID tmprid = curRid2;
      

        //    	printf("outter loop\n" );
            	while(tupleCmp(curRecPtr1,curRecPtr2)==0 &&scanstatus2==OK){
      //      		printf("inner loop\n" );
            		// put into joinfed file
            		// if(curRecPtr2!=NULL)
     //        			printf("before join s is : %d\n", ((struct _rec *) curRecPtr2)->key);
					char *tuple = new char[16];
		            memcpy(tuple, curRecPtr1, curlen1);
		            memcpy(tuple + 8, curRecPtr2, curlen2);
		            joinedfile->insertRecord(tuple, 16, dumb);
		            // printf("joined\n" );
		            delete tuple;
		            scanstatus2 = scan2->getNext(curRid2, curRecPtr2, curlen2);
		            // printf("S moved down\n" );

            	}

      //      	if(curRecPtr2!=NULL)
       //     		printf("after jjoin s is : %d\n", ((struct _rec *) curRecPtr2)->key);

            	scanstatus1	=  scan1->getNext(curRid1, curRecPtr1, curlen1);
          //  	printf("R moved down\n" );
            	scan2->position(tmprid);
            	
            	curRid2 = tmprid;
            	scanstatus2 = scan2->getNext(curRid2, curRecPtr2, curlen2);
            //	printf("S reset\n" );

            }
		}

	}

	delete sortedfile1;
	delete sortedfile2;
	delete curRecPtr1;
	delete curRecPtr2;

	delete scan1;
	delete scan2;
    MINIBASE_DB -> delete_file_entry(sortedfilename1);
    MINIBASE_DB -> delete_file_entry(sortedfilename2);

    s = OK;
}

// sortMerge destructor
sortMerge::~sortMerge()
{
	// fill in the body
}
