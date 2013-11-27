#include "catalog.h"
#include "query.h"
#include "sort.h"
#include "index.h"
#include <stdlib.h>
#include <cstring>

Status Operators::SNL(const string& result,           // Output relation name
                      const int projCnt,              // Number of attributes in the projection
                      const AttrDesc attrDescArray[], // Projection list (as AttrDesc)
                      const AttrDesc& attrDesc1,      // The left attribute in the join predicate
                      const Operator op,              // Predicate operator
                      const AttrDesc& attrDesc2,      // The left attribute in the join predicate
                      const int reclen)               // The length of a tuple in the result relation
{
  cout << "Algorithm: Simple NL Join" << endl;

  /* Your solution goes here */
  /*
	From wikipedia:

	For each tuple r in R do
	     For each tuple s in S do
	        If r and s satisfy the join condition
	           Then output the tuple <r,s>

  */

	Status returnStatus;
	AttrDesc* rel_attr_1;
	AttrDesc* rel_attr_2;
	int attrCnt_1;
	int attrCnt_2;

	RID rid1;
	RID rid2;
	Record record1;
	Record record2;

  returnStatus = attrCat->getRelInfo(attrDesc1.relName, attrCnt_1, rel_attr_1);
  if(returnStatus != OK){
    return returnStatus;
  }

  returnStatus = attrCat->getRelInfo(attrDesc2.relName, attrCnt_2, rel_attr_2);
  if(returnStatus != OK){
    return returnStatus;
  }


	HeapFile* heapFile = new HeapFile(result, returnStatus);

  //relation 2 loop scan

  HeapFileScan *attrDesc2_scan = new HeapFileScan(attrDesc2.relName, attrDesc2.attrOffset, 
                                  attrDesc2.attrLen, (Datatype)attrDesc2.attrType, 
                                  NULL, op, returnStatus);

  if(returnStatus != OK){
        return returnStatus;
  }

  //scan through all tuples in relation 2

  while( (returnStatus = attrDesc2_scan->scanNext(rid2, record2)) == OK) {

    char * attr2_filter = new char [attrDesc2.attrLen];

    memcpy(attr2_filter, (char*) record2.data + attrDesc2.attrOffset, attrDesc2.attrLen);

    //relation 1 loop scan
    HeapFileScan *attrDesc1_scan = new HeapFileScan(attrDesc1.relName, attrDesc1.attrOffset, 
                                    attrDesc1.attrLen, (Datatype)attrDesc1.attrType, 
                                    attr2_filter, op, returnStatus);
    if(returnStatus != OK){
          return returnStatus;
    }
//    cout << "first attr = " << *(int*) (record2.data + attrDesc2.attrOffset) << endl;
  	while( (returnStatus = attrDesc1_scan->scanNext(rid1, record1)) == OK) {

      Record record;
      record.data = malloc(reclen);
      record.length = reclen;
      RID rid;

      int offset = 0;

      for(int i = 0; i < projCnt; i++){

        if(!strcmp(attrDescArray[i].relName, attrDesc1.relName)){
          memcpy((char *)record.data + offset, (char *)record1.data + attrDescArray[i].attrOffset, attrDescArray[i].attrLen);
          offset += attrDescArray[i].attrLen;   
        }
        else if(!strcmp(attrDescArray[i].relName, attrDesc2.relName)){
          memcpy((char *)record.data + offset, (char *)record2.data + attrDescArray[i].attrOffset, attrDescArray[i].attrLen);
          offset += attrDescArray[i].attrLen;   
        }
        else{
          //we should never reach this point
          cout <<"Could not find projected attribute"<<endl;
        }
      }

      returnStatus = heapFile->insertRecord(record, rid);
      if(returnStatus != OK){
        return returnStatus;
      }

      free(record.data);
   	}
    delete attrDesc1_scan;
    delete attr2_filter;
 	}

  delete attrDesc2_scan;
  delete heapFile;
  return OK;
}

