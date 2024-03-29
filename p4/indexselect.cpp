#include "catalog.h"
#include "query.h"
#include "index.h"
#include <stdlib.h>
#include <cstring>

Status Operators::IndexSelect(const string& result,       // Name of the output relation
                              const int projCnt,          // Number of attributes in the projection
                              const AttrDesc projNames[], // Projection list (as AttrDesc)
                              const AttrDesc* attrDesc,   // Attribute in the selection predicate
                              const Operator op,          // Predicate operator
                              const void* attrValue,      // Pointer to the literal value in the predicate
                              const int reclen)           // Length of a tuple in the output relation
{
  cout << "Algorithm: Index Select" << endl;

  /* Your solution goes here */
  //Initialize index and Heap
  Status returnStatus;
  string relName(attrDesc->relName);
  Index *index = new Index(relName, attrDesc->attrOffset, attrDesc->attrLen, (const Datatype)(attrDesc->attrType), 0, returnStatus);

  if(returnStatus != OK){
    delete index;
    return returnStatus;
  }
  //HeapFileScan *scan = new HeapFileScan((*attrDesc).relName, returnStatus);
  // Filtered Scan
  HeapFileScan *scan = new HeapFileScan(relName, attrDesc->attrOffset, 
                                  attrDesc->attrLen, (Datatype)attrDesc->attrType, 
                                  (char*)attrValue, op, returnStatus);

  HeapFile heapfile(result, returnStatus);
  if(returnStatus != OK){
    delete scan;
    delete index;
  	return returnStatus;
  }
//Start Scan --> if input = attrValue....

  if((returnStatus = index->startScan(attrValue)) == OK){
    RID rid;
    RID heaprid;
    Record record;

    while((returnStatus = index->scanNext(rid)) == OK){

        returnStatus = scan->getRandomRecord(rid, record);
        if(returnStatus != OK){
          delete scan;
          delete index;
          return returnStatus;
        }

        Record new_record;
        new_record.length = 0;
        new_record.data = malloc(reclen);
        
        for (int j=0; j<projCnt; j++) {
          //creating output record
          memcpy((char *)new_record.data + new_record.length, (char *)record.data + projNames[j].attrOffset, projNames[j].attrLen);
          new_record.length +=  projNames[j].attrLen;                                                
        }

        returnStatus = heapfile.insertRecord(new_record,rid); //insert output into final heapfile relation
        if (returnStatus != OK) {
            free(new_record.data);
            delete scan;
            delete index;
            return returnStatus;
        }
        free(new_record.data);    
    }
  }

  index->endScan();
  delete scan;
  delete index;

  return OK;
}

