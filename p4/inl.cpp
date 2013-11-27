#include "catalog.h"
#include "query.h"
#include "sort.h"
#include "index.h"
#include <stdlib.h>
#include <cstring>
/* 
 * Indexed nested loop evaluates joins with an index on the 
 * inner/right relation (attrDesc2)
 */

Status Operators::INL(const string& result,           // Name of the output relation
                      const int projCnt,              // Number of attributes in the projection
                      const AttrDesc attrDescArray[], // The projection list (as AttrDesc)
                      const AttrDesc& attrDesc1,      // The left attribute in the join predicate
                      const Operator op,              // Predicate operator
                      const AttrDesc& attrDesc2,      // The left attribute in the join predicate
                      const int reclen)               // Length of a tuple in the output relation
{
  cout << "Algorithm: Indexed NL Join" << endl;

  /* Your solution goes here */


  //scan through all tuples in relation 2

  Status returnStatus;
	AttrDesc* rel_attr_1;
	AttrDesc* rel_attr_2;
	int attrCnt_1;
	int attrCnt_2;

	RID rid1;
	RID rid2;
	Record record1;
	Record record2;

	cout<<"Check 1"<<endl;
  returnStatus = attrCat->getRelInfo(attrDesc1.relName, attrCnt_1, rel_attr_1);
  if(returnStatus != OK){
    return returnStatus;
  }

	cout<<"Check 2"<<endl;
  returnStatus = attrCat->getRelInfo(attrDesc2.relName, attrCnt_2, rel_attr_2);
  if(returnStatus != OK){
    return returnStatus;
  }


	HeapFile* heapFile = new HeapFile(result, returnStatus);

  //relation 2 loop scan

  HeapFileScan *attrDesc2_scan = new HeapFileScan(attrDesc2.relName, attrDesc2.attrOffset, 
                                  attrDesc2.attrLen, (Datatype)attrDesc2.attrType, 
                                  NULL, op, returnStatus);

	cout<<"Check 3"<<endl;
  if(returnStatus != OK){
        return returnStatus;
  }
  HeapFileScan *attrDesc1_scan = new HeapFileScan(attrDesc1.relName, attrDesc1.attrOffset, 
                                  attrDesc1.attrLen, (Datatype)attrDesc1.attrType, 
                                  NULL, op, returnStatus);

	cout<<"Check 4"<<endl;
  if(returnStatus != OK){
        return returnStatus;
  }

  //scan through all tuples in relation 2

  Index* index;
  index = new Index(attrDesc1.relName, attrDesc1.attrOffset, attrDesc1.attrLen, (Datatype)attrDesc1.attrType, 0, returnStatus);

	cout<<"Check 5"<<endl;
  if(returnStatus!= OK) {
   	return returnStatus;
  }

  while( (returnStatus = attrDesc2_scan->scanNext(rid2, record2)) == OK) {

    char * attr2_filter = new char [attrDesc2.attrLen];

    memcpy(attr2_filter, (char*) record2.data + attrDesc2.attrOffset, attrDesc2.attrLen);

    cout << "first attr = " << *(int*) (record2.data + attrDesc2.attrOffset) << endl;
    //relation 1 loop scan
    /* HeapFileScan *attrDesc1_scan = new HeapFileScan(attrDesc1.relName, attrDesc1.attrOffset, 
                                    attrDesc1.attrLen, (Datatype)attrDesc1.attrType, 
                                    attr2_filter, op, returnStatus);
    if(returnStatus != OK){
          return returnStatus;
    } */
		

   
    returnStatus = index -> startScan(attr2_filter);
		cout<<"Check 6"<<endl;
    if(returnStatus != OK) {
    	return returnStatus;
    }
  	while(( returnStatus = index -> scanNext(rid1)) == OK) {

  		returnStatus = attrDesc1_scan ->getRandomRecord(rid1, record1);

			cout<<"Check 7"<<endl;

  		if(returnStatus != OK) {
  			return returnStatus;
  		}

      Record record;
      record.data = malloc(reclen);
      record.length = reclen;
      RID rid;

      int offset = 0;

      for(int i = 0; i < projCnt; i++){

        if(strcmp(attrDescArray[i].relName, attrDesc1.relName)){
          memcpy((char *)record.data + offset, (char *)record1.data + attrDescArray[i].attrOffset, attrDescArray[i].attrLen);
          offset += attrDescArray[i].attrLen;   
        }
        else if(strcmp(attrDescArray[i].relName, attrDesc2.relName)){
          memcpy((char *)record.data + offset, (char *)record2.data + attrDescArray[i].attrOffset, attrDescArray[i].attrLen);
          offset += attrDescArray[i].attrLen;   
        }
        else{
          //we should never reach this point
          cout <<"Could not find projected attribute"<<endl;
        }
      }

      returnStatus = heapFile->insertRecord(record, rid);
			cout<<"Check 8"<<endl;
      if(returnStatus != OK){
        return returnStatus;
      }

      free(record.data);
   	}
    delete attr2_filter;
 	}

	cout<<"Check 9"<<endl;

  index->endScan();
  delete attrDesc1_scan;
  delete attrDesc2_scan;
  delete heapFile;
  delete index;

  return OK;
}

