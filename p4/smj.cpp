#include "catalog.h"
#include "query.h"
#include "sort.h"
#include "index.h"

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
  int sortPages = .8*unpinnedPages;
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
  int maxItems1 = sortPages/reclen1;
  int maxItems2 = sortPages/reclen2;
  Status returnStatus1;
  Status returnStatus2;
  SortedFile* sortedFile1 = new SortedFile(relName1, attrDesc1.attrOffset, 
  							attrDesc1.attrLen, (Datatype) attrDesc1.attrType, 
							maxItems1, returnStatus1);
  SortedFile* sortedFile2 = new SortedFile(relName2, attrDesc2.attrOffset, 
  							attrDesc2.attrLen, (Datatype) attrDesc2.attrType, 
							maxItems2, returnStatus2);
       

  return OK;
}

