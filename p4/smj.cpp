#include "catalog.h"
#include "query.h"
#include "sort.h"
#include "index.h"
#include <stdlib.h>
#include <cstring>
/* Consider using Operators::matchRec() defined in join.cpp
 * to compare records when joining the relations */
  
Status Operators::SMJ(const string& result,           // Output relation name
                      const int projCnt,              // Number of attributes in the projection
                      const AttrDesc attrDescArray[], // Projection list (as AttrDesc)
                      const AttrDesc& attrDesc1,      // The left attribute in the join predicate
                      const Operator op,              // Predicate operator
                      const AttrDesc& attrDesc2,      // The left attribute in the join predicate
                      const int reclen)               // The length of a tuple in the result relation
{
  cout << "Algorithm: SM Join" << endl;

  /* Your solution goes here */
  int unpinnedPages = bufMgr->numUnpinnedPages();

  double sortPages = .8*unpinnedPages;

  RelDesc relDesc1;
  RelDesc relDesc2;
  string relName1(attrDesc1.relName);
  string relName2(attrDesc2.relName);

  int reclen1 = 0;
  int reclen2 = 0;
  
  AttrDesc* leftAttrs;
  AttrDesc* rightAttrs;

  // Get relation info from relCat
  relCat->getInfo(relName1, relDesc1);
  relCat->getInfo(relName2, relDesc2);

  attrCat->getRelInfo(relName1, relDesc1.attrCnt, leftAttrs);
  attrCat->getRelInfo(relName2, relDesc2.attrCnt, rightAttrs);

  // Calculate size of record for each attribute in join condition
  for(int i = 0 ; i < relDesc1.attrCnt; i++) {
  	reclen1 += leftAttrs[i].attrLen;
  }
  for(int i = 0 ; i < relDesc2.attrCnt; i++) {
  	reclen2 += rightAttrs[i].attrLen;
  }

  // Sort each relation in the join

  int maxItems1 = (sortPages/reclen1)*1024;
  int maxItems2 = (sortPages/reclen2)*1024;

  Status returnStatus1;
  Status returnStatus2;

  SortedFile* sortedFile1 = new SortedFile(attrDesc1.relName, attrDesc1.attrOffset, 
  							attrDesc1.attrLen, (Datatype) attrDesc1.attrType, 
							maxItems1, returnStatus1);

  if(returnStatus1 != OK){
    return returnStatus1;
  }

  SortedFile* sortedFile2 = new SortedFile(attrDesc2.relName, attrDesc2.attrOffset, 
  							attrDesc2.attrLen, (Datatype) attrDesc2.attrType, 
							maxItems2, returnStatus2);

  if(returnStatus2 != OK){
    return returnStatus2;
  }

  Record record1;
  Record record2;
  Record record3;

  RID rid1;
  RID rid2;
  RID rid3;

  HeapFile* heapFile = new HeapFile(result, returnStatus1);
  if(returnStatus1 != OK){
    return returnStatus1;
  }

  returnStatus1 = sortedFile1->next(record1);

  if(returnStatus1 != OK){
    return returnStatus1;
  }

  returnStatus2 = sortedFile2->next(record2);

  if(returnStatus2 != OK){
    return returnStatus2;
  }


  while(true){

    if(matchRec(record1, record2, attrDesc1, attrDesc2) < 0){
      returnStatus1 = sortedFile1->next(record1);
      if(returnStatus1 != OK){
        break;
      }
    }
    else if(matchRec(record1, record2, attrDesc1, attrDesc2) > 0){
      returnStatus2 = sortedFile2->next(record2);
      if(returnStatus2 != OK){
        break;
      }
    }
    else{
      record3 = record1;
      returnStatus1 = sortedFile1->setMark();
      if(returnStatus1 != OK){
        return returnStatus1;
      }

      bool cont = true;
      while(matchRec(record3, record2, attrDesc1, attrDesc2) == 0 && cont){
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

        returnStatus1 = heapFile->insertRecord(record, rid);
        if(returnStatus1 != OK){
          return returnStatus1;
        }
        returnStatus1 = sortedFile1->next(record3);
        if(returnStatus1 != OK) {
          cont = false;
        }
        free(record.data);
      }
      returnStatus2 = sortedFile2->next(record2);
      if(returnStatus2 != OK){
        break;
      }

      returnStatus1 = sortedFile1->gotoMark();
      if(returnStatus1 != OK){
        return returnStatus1;
      }
    }
  }

  delete sortedFile1;
  delete sortedFile2;
  delete heapFile;
  return OK;
}

