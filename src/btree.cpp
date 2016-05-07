/**
 * @author See Contributors.txt for code contributors and overview of BadgerDB.
 *
 * @section LICENSE
 * Copyright (c) 2012 Database Group, Computer Sciences Department, University of Wisconsin-Madison.
 */

#include "btree.h"
#include "filescan.h"
#include "exceptions/bad_index_info_exception.h"
#include "exceptions/bad_opcodes_exception.h"
#include "exceptions/bad_scanrange_exception.h"
#include "exceptions/no_such_key_found_exception.h"
#include "exceptions/scan_not_initialized_exception.h"
#include "exceptions/index_scan_completed_exception.h"
#include "exceptions/file_not_found_exception.h"
#include "exceptions/end_of_file_exception.h"
#include "exceptions/page_not_pinned_exception.h"
//#include <iostream>
//#include <memory>

//#define DEBUG

namespace badgerdb
{

// -----------------------------------------------------------------------------
// BTreeIndex::BTreeIndex -- Constructor
// -----------------------------------------------------------------------------

BTreeIndex::BTreeIndex(const std::string & relationName,
		std::string & outIndexName,
		BufMgr *bufMgrIn,
		const int attrByteOffset,
		const Datatype attrType)
{
	//first, check if the index file exists
	//if the file exists, open it
	//if not, create a new file
	//need to do somethi9ngn different for each 3 types of int, double and string
	std::cout << "input parameters\n";
	std::cout << "relationName: " << relationName << "\n";
	std::cout << "outIndexName: " << outIndexName << "\n";
	std::cout << "bufMgrIn: " << bufMgrIn << "\n";
	std::cout << "attrByteOffset: " << attrByteOffset << "\n";
	std::cout << "attrType: " << attrType << "\n";
	bufMgr = bufMgrIn;
	this->attrByteOffset = attrByteOffset;
	attributeType = attrType;
	scanExecuting = false;


	//do the following depending on the attribute type of string, 
	//int or double
	//**********from the project spec*********
	std::ostringstream idxStr;
	idxStr << relationName << '.' << attrByteOffset;
	outIndexName = idxStr.str() ; // w is the name of the index file

	leafOccupancy = 0;
	nodeOccupancy = 0;
	if (attributeType == INTEGER){
		//0 = interger attribute type
		nodeOccupancy = INTARRAYNONLEAFSIZE;
		leafOccupancy = INTARRAYLEAFSIZE;
	} 
	else if (attributeType == DOUBLE){
		//1 = double attribute type
		nodeOccupancy = DOUBLEARRAYNONLEAFSIZE;
		leafOccupancy = DOUBLEARRAYLEAFSIZE;
	} 
	else if (attributeType == STRING){
		//2 = string attribute type
		nodeOccupancy = STRINGARRAYNONLEAFSIZE;
		leafOccupancy = STRINGARRAYLEAFSIZE;	
	}
	else {
		//should never occur
		std::cout << "none of the attribute types matched";
		//throw exception
	}
		


	try{
		// if file already exists
		this->file = new BlobFile(outIndexName, false);
		Page* currPage;
		headerPageNum = 1;
		bufMgr->readPage(file, headerPageNum, currPage);
		IndexMetaInfo* metaData = (IndexMetaInfo*) currPage;

		
		if(metaData->attrByteOffset != attrByteOffset|| metaData->attrByteOffset != attributeType 
			|| strcmp(metaData->relationName, relationName.c_str()) != 0
				){
			try{ // if metadata does not match with existing file, unpin page
				bufMgr->unPinPage(file, headerPageNum, false);
			} catch (PageNotPinnedException e ) {
				// do nothing
			}

			throw BadIndexInfoException("metadata from constructor does not match existing file's metadata");
		}

		rootPageNum = metaData->rootPageNo;
		try {
			bufMgr->unPinPage(file, headerPageNum, false);
		} catch (PageNotPinnedException e){

			// do nothing
		}
	}
	catch (FileNotFoundException e){ // if a new file must be created
		file = new BlobFile(outIndexName, true);
		Page* currPage;
		bufMgr->allocPage(file, headerPageNum, currPage);
		// set metadata:
		IndexMetaInfo* metaData = (IndexMetaInfo*) currPage;
		metaData->attrByteOffset = attrByteOffset;
		metaData->attrType = attributeType;
		strcpy(metaData->relationName, relationName.c_str());
		// unpinpage if it is already pinned

		// call allocPage in order to set the rootPageNo for the metadata
		bufMgr->allocPage(file, rootPageNum, currPage);
		metaData->rootPageNo = rootPageNum;


		try{
			bufMgr->unPinPage(file, headerPageNum, true);
		} catch (PageNotPinnedException e) {
			// do nothing
		}

		try{
			bufMgr->unPinPage(file, rootPageNum, true);
		} catch (PageNotPinnedException e ) {
			// do nothing
		}

				// set the pageNoArray = 0 (not invalid)
		if(attributeType == INTEGER){
			NonLeafNodeInt *currNode = (NonLeafNodeInt *) currPage;
			for(int i = 0; i < nodeOccupancy; i++){
				currNode->pageNoArray[i] = 0;
			} 
			currNode->level = 1;
		} else if(attributeType == DOUBLE){
			NonLeafNodeDouble *currNode = (NonLeafNodeDouble *) currPage;
			for(int i = 0; i < nodeOccupancy; i++){
				currNode->pageNoArray[i] = 0;
			} 
			currNode->level = 1;
		} else if (attributeType == STRING) {
			NonLeafNodeString *currNode = (NonLeafNodeString *) currPage;
			for(int i = 0; i < nodeOccupancy; i++){
				currNode->pageNoArray[i] = 0;
			} 
			currNode->level = 1;
		}
		FileScan fscan(relationName, bufMgr);
		try{
		// file scan taken from main.cpp:
			FileScan fscan(relationName, bufMgr);
			RecordId scanRid;
			bool cont = true;
			while(cont)
			{
				fscan.scanNext(scanRid);
				std::string recordStr = fscan.getRecord();
				const char *record = recordStr.c_str();
				insertEntry((void *) (record + attrByteOffset), scanRid);
			}
		}catch(EndOfFileException e){
			// end of scan
		}
		//delete fscan;
}
}


// -----------------------------------------------------------------------------
// BTreeIndex::~BTreeIndex -- destructor
// -----------------------------------------------------------------------------

BTreeIndex::~BTreeIndex()
{
	//flip the scanExecuting bool
	scanExecuting = false;
	//unpin btree pages that re pinned
	//for(int i = 0; i < bufMgr.bufPool.size(); i++)
	//{
	//	bufMgr->unpinpage(i);
	//}
	//flush the buffer and exit
	bufMgr->flushFile(file);
	delete file; // called to trigger the blobfile class's destructor
}

// -----------------------------------------------------------------------------
// BTreeIndex::insertEntry
// -----------------------------------------------------------------------------

const void BTreeIndex::insertEntry(const void *key, const RecordId rid) 
{
	if(scanExecuting){
		endScan();
	}

	if( attributeType == INTEGER)
	{
		RIDKeyPair<int> entry;
		entry.set(rid, *((int* )key));
		PageId updateId = 0;
		int newNodeVal = 0;
		insertInteger(entry, false, rootPageNum, updateId, newNodeVal);
		// root split in recursive call:
		if(updateId == 0){
			Page* root;
			bufMgr->allocPage(file, updateId, root);
			NonLeafNodeInt* rootNode = (NonLeafNodeInt*) root;
			for(int i = 0; i < nodeOccupancy + 1; i++){
				rootNode->pageNoArray[i] = 0;
			}
			rootNode->keyArray[0] = newNodeVal;
		}
	} 
	else if (attributeType == DOUBLE)
	{
		RIDKeyPair<double> entry;
		entry.set(rid, *((double* )key));
		insertDouble(entry, false, rootPageNum);
	}
	else if (attributeType == STRING)
	{
		RIDKeyPair<char[STRINGSIZE]> entry;
		//entry.set(rid, *((char* )key));
		insertString(entry, false, rootPageNum);
	}
}


// ----------------------------------------------------------------------------
//BTreeIndex::insertInteger
//
// ---------------------------------------------------------------------------
const void BTreeIndex::insertInteger(RIDKeyPair<int> entry, bool leaf, PageId pageNo, PageId updatedId, int newValInt) 
{

// TODO: (may need a different method for this or what is below) NEW ROOT NODE

	// for a non-leaf node
	if(!leaf){
		insertNonLeafInteger(entry, newValInt, leaf, pageNo, updatedId);
	}
	// for a leaf node:
	else {
		insertLeafInteger(entry, leaf, pageNo, updatedId, newValInt);

	}


}


// ----------------------------------------------------------------------------
//BTreeIndex::insertInteger
//
// ---------------------------------------------------------------------------
const void BTreeIndex::insertNonLeafInteger(RIDKeyPair<int> entry, PageId pageNo, bool leaf, int newNodeVal, PageId& updatedId) 
{	
	Page* newerPage;
	bufMgr->readPage(file, pageNo, newerPage);
	NonLeafNodeInt* currentNode = (NonLeafNodeInt*) newerPage;
	
	// find position
	int i = 0;
	while(currentNode->keyArray[i] < entry.key && i < nodeOccupancy && currentNode->pageNoArray[i + 1] != 0){
		i++;
	}
	if(currentNode->pageNoArray[i] == 0){ // create leaf since there are no more positions
		Page* newPage;
		PageId newPageId;
		bufMgr->allocPage(file, newPageId, newPage);
		LeafNodeInt* newNode = (LeafNodeInt*) newPage;
		for(int j = 0; j < nodeOccupancy; j++){
			newNode->ridArray[j].page_number = 0;
		}
		newNode->ridArray[0] = entry.rid;
		newNode->rightSibPageNo = 0;
		currentNode->pageNoArray[0] = newPageId;
		bufMgr->unPinPage(file, newPageId, true);
		bufMgr->unPinPage(file, pageNo, true);
		return; // done with inserting
	}

	bufMgr->unPinPage(file, pageNo, false);
	bool isLeaf = true;
	if(currentNode->level == 1){
		isLeaf = true;
	}
	else{
		isLeaf = false;
	}
	PageId newNodeId = 0;
	int newVal = 0;
	// recursive call:
	insertInteger(entry, isLeaf, currentNode->pageNoArray[i], newVal, newNodeId);


	// create new NonLeafNode 
	//if node was not split:
	if(newNodeId == 0){
		// node was already placed in recursion
	}
	else{ // child was split: so new values were filled
		bufMgr->readPage(file, pageNo, newerPage);
		NonLeafNodeInt* currentNode = (NonLeafNodeInt*) newerPage;
		//check if array is full:
		int b = 0;
		bool smaller;
		for(b = 0; b < nodeOccupancy; b++){
			if(currentNode->pageNoArray[b + 1] == 0){
				break;
			}
			if(b < nodeOccupancy){
				smaller = true;
			}
			else{
				smaller = false;
			}
		}
		// not full so can insert
		if(smaller){
			// must move over values to put in new one: found position (i) earlier
			for(int c = b; c > i; c--){
				currentNode->pageNoArray[c] = currentNode->pageNoArray[c - 1];
				currentNode->keyArray[c] = currentNode->keyArray[c - 1];
			}
			currentNode->pageNoArray[i + 1] = newNodeId;
				// currNode->keyArray[c] = the new value from splitting functions **************
		}
		// full so must split nonleaf
		else{
				// TODO: SPLIT **********************************************
		}

		nodeOccupancy++;
		bufMgr->unPinPage(file, pageNo, true);
	}
}


// ----------------------------------------------------------------------------
//BTreeIndex::insertInteger
//
// ---------------------------------------------------------------------------
const void BTreeIndex::insertLeafInteger(RIDKeyPair<int> entry, bool leaf, 
			PageId pageNo, PageId& updatedId, int & nodeValue)
{	
	Page* currPage;
	bufMgr->readPage(file, pageNo, currPage);
	LeafNodeInt* currNode = (LeafNodeInt*) currPage;
	// find the position 
	int i = 0;
	while(currNode->keyArray[i] < entry.key && i < leafOccupancy && currNode->ridArray[i].page_number != 0){
		i++;
	}
	// find last entry in the array
	int jLast = 0;
	while(jLast < leafOccupancy && currNode->ridArray[i].page_number != 0){
		jLast++;
	}
	// if the array is not full, move over recordIds in array to place new record in the correct position
	int a = 0;
	if(jLast < leafOccupancy){
	for(a = jLast; a > i; a--){
			currNode->ridArray[a] = currNode->ridArray[a - 1];
			currNode->keyArray[a] = currNode->keyArray[a - 1];
		}
		currNode->ridArray[a] = entry.rid;
		currNode->keyArray[a] = entry.key;
	}
	else{ //if full:
		splitLeaf(i, entry, currNode, updatedId, nodeValue);

	}
	leafOccupancy++;
	bufMgr->unPinPage(file, pageNo, true);



}

// -----------------------------------------------------------------------------
// BTreeIndex::splitNode
// -----------------------------------------------------------------------------

const void BTreeIndex::splitNode(int position, NonLeafNodeInt* currNode, PageId& updatedId, int & splitVal, PageId pageNo) 
{

								/*	(int pos, // position
									 int NONLEAFARRAYMAX,
									 T2* nonLeafNode,: // currNode
									 PageId& newPageId, // updatedId
									 T & newValue,
									 T&  newChildValue, // value sent into recursion method
									 PageId newChildPageId){ // new node ID (last entry into recursion call).
									 	*/

	Page* nodePage;
	bufMgr->allocPage(file, updatedId, nodePage);
	NonLeafNodeInt* newNode = (NonLeafNodeInt*) nodePage;

	//tmp array

	int keyPtr[nodeOccupancy + 1]; 
	PageId idPtr[nodeOccupancy + 2];

	//copy to tmp
	for(int i = 0; i < nodeOccupancy; i++) {
		idPtr[i] = currNode->pageNoArray[i];
		keyPtr[i] = currNode->keyArray[i];
	//	int temp = keyPtr[i];
		keyPtr[i] = currNode->keyArray[i];
	}
	idPtr[nodeOccupancy + 1] = currNode->pageNoArray[nodeOccupancy + 1];


	for(int i= nodeOccupancy; i > position; i--){
		keyPtr[i] = keyPtr[i-1];
		idPtr[i + 1] = idPtr[i];
	}
	keyPtr[position] = splitVal;
	idPtr[position + 1] = pageNo;

	//clear old and new page
	for(int i=0; i < nodeOccupancy + 1; i++){
		currNode->pageNoArray[i] = 0;
		newNode->pageNoArray[i] = 0;
	}

	//copy back
	for(int i=0; i< (nodeOccupancy + 1) / 2; i++ ){
		currNode->keyArray[i] = keyPtr[i];
		currNode->pageNoArray[i] = idPtr[i];
	}
	currNode->pageNoArray[(nodeOccupancy + 1) / 2] = idPtr[(nodeOccupancy + 1) / 2];

	for(int i= (nodeOccupancy + 1) / 2 + 1; i < nodeOccupancy + 1; i++){
		newNode->keyArray[i - (nodeOccupancy + 1) / 2 - 1] = keyPtr[i];
		newNode->pageNoArray[i - (nodeOccupancy + 1) / 2 - 1 ] = idPtr[i];
	}
	newNode->pageNoArray[nodeOccupancy + 1 - (nodeOccupancy + 1) / 2 - 1 ] = idPtr[nodeOccupancy + 1];

	//level
	newNode->level = currNode->level;

	//push up
	splitVal =  keyPtr[(nodeOccupancy + 1) / 2];

	//unpin
	bufMgr->unPinPage(file, updatedId, true);


}

// -----------------------------------------------------------------------------
// BTreeIndex::splitNode
// -----------------------------------------------------------------------------

const void BTreeIndex::splitLeaf(int position, RIDKeyPair<int> entry, 
	LeafNodeInt* currNode, PageId& updatedId, int & leafValue){

	//full
	Page* nodePage;
	bufMgr->allocPage(file, updatedId, nodePage);
	LeafNodeInt* newNode = (LeafNodeInt *) nodePage;

	//tmp array.
	int keyPtr[nodeOccupancy + 1];
	RecordId idPtr[nodeOccupancy + 2];

	//copy all new records to tmp
	for(int i = 0; i < leafOccupancy; i++) {
		idPtr[i] = currNode->ridArray[i];
		keyPtr[i] = currNode->keyArray[i];
		currNode->ridArray[i].page_number = 0;
	}

	for(int j = leafOccupancy; j > position; j--){
		keyPtr[j] = keyPtr[j - 1];
		idPtr[j] = idPtr[j - 1];
	}
	keyPtr[position] = entry.key;
	idPtr[position] = entry.rid;

	//reset new and old page

	for(int k = 0; k < leafOccupancy; k++) {
		currNode->ridArray[k].page_number = 0;
		newNode->ridArray[k].page_number = 0;
	}

	//copy back
	for(int l = 0; l < (leafOccupancy + 1) / 2; l++){
		currNode->keyArray[l] = keyPtr[l];
		currNode->ridArray[l] = idPtr[l];
	}

	for(int m = (leafOccupancy + 1) / 2; m < leafOccupancy + 1; m++){
		newNode->keyArray[m - (leafOccupancy + 1) / 2] = keyPtr[m];
		newNode->ridArray[m - (leafOccupancy + 1) / 2] = idPtr[m];
	}

	//link leaf node
	newNode->rightSibPageNo = currNode->rightSibPageNo;
	currNode->rightSibPageNo = updatedId;

	//push up
	leafValue = newNode->keyArray[0];

	//unpin
	bufMgr->unPinPage(file, updatedId, true);


} 


// ----------------------------------------------------------------------------
//BTreeIndex::insertDouble
//
// ---------------------------------------------------------------------------
const void BTreeIndex::insertDouble(RIDKeyPair<double> entry, bool leaf, PageId pageNo) 
{
	// for a leaf node:
	//PageId* currPagegNum;
	if(leaf){
		Page* currPage;
		bufMgr->readPage(file, pageNo, currPage);
		LeafNodeDouble* currNode = (LeafNodeDouble*) currPage;
		// find the position 
		int i = 0;
		while(currNode->keyArray[i] < entry.key && i < leafOccupancy && currNode->ridArray[i].page_number != 0){
			i++;
		}
		// find last entry in the array
		int jLast = 0;
		while(jLast < leafOccupancy && currNode->ridArray[i].page_number != 0){
			jLast++;
		}
		// if the array is not full, move over recordIds in array to place new record in the correct position
		if(jLast < leafOccupancy){
			for(int a = jLast; a > i; a--){
				currNode->ridArray[a] = currNode->ridArray[a - 1];
				currNode->keyArray[a] = currNode->keyArray[a - 1];
			}
		}
		else{ //if full:
		// TODO: must split ***************
		}
		leafOccupancy++;
		bufMgr->unPinPage(file, pageNo, true);
	}
	// for a non-leaf node
	else{
		Page* newerPage;
		bufMgr->readPage(file, pageNo, newerPage);
		NonLeafNodeDouble* currNode = (NonLeafNodeDouble*) newerPage;
		// find position
		int i = 0;
		while(currNode->keyArray[i] < entry.key && i < nodeOccupancy && currNode->pageNoArray[i + 1] != 0){
			i++;
		}
		if(currNode->pageNoArray[i] == 0){ // create leaf since there are no more positions
			Page* newPage;
			PageId updatedId;
			bufMgr->allocPage(file, updatedId, newPage);
			LeafNodeDouble* newNode = (LeafNodeDouble*) newPage;
			for(int j = 0; j < nodeOccupancy; j++){
				newNode->ridArray[j].page_number = 0;
			}
			newNode->ridArray[0] = entry.rid;
			newNode->rightSibPageNo = 0;
			currNode->pageNoArray[0] = updatedId;
			bufMgr->unPinPage(file, updatedId, true);
			bufMgr->unPinPage(file, pageNo, true);
			return; // done with inserting
		}

		bufMgr->unPinPage(file, pageNo, false);
		bool isLeaf = true;
		if(currNode->level == 1){
			isLeaf = true;
		}
		else{
			isLeaf = false;
		}
		insertDouble(entry, isLeaf, currNode->pageNoArray[i]);

		// TODO: check if node was split. if so, must insert *****************	
		// 


	}
	nodeOccupancy++;
	bufMgr->unPinPage(file, pageNo, true);

}


// ----------------------------------------------------------------------------
//BTreeIndex::insertString
//
// ---------------------------------------------------------------------------
const void BTreeIndex::insertString(RIDKeyPair<char[STRINGSIZE]> entry, bool leaf, PageId pageNo) 
{
/*	// for a leaf node:
	PageId* currPageNum;
	if(leaf){
		Page* currPage;
		bufMgr->readPage(file, pageNo, currPage);
		LeafNodeString* currNode = (LeafNodeString*) currPage;
		// find the position 
		int i = 0;
		while(currNode->keyArray[i] < entry.key && i < leafOccupancy && currNode->ridArray[i].page_number != 0){
			i++;
		}
		// find last entry in the array
		int jLast = 0;
		while(jLast < leafOccupancy && currNode->ridArray[i].page_number != 0){
			jLast++;
		}
		// if the array is not full, move over recordIds in array to place new record in the correct position
		if(jLast < leafOccupancy){
			for(int a = jLast; a > i; a--){
	//			currNode->ridArray[a] = currNode->ridArray[a - 1];
	//			currNode->keyArray[a] = currNode->keyArray[a - 1];
			}
		}
		else{ //if full:
		// TODO: must split ***************
		}
		leafOccupancy++;
		bufMgr->unPinPage(file, pageNo, true);
	}
	// for a non-leaf node
	else{
		Page* newerPage;
		bufMgr->readPage(file, pageNo, newerPage);
		NonLeafNodeString* currNode = (NonLeafNodeString*) newerPage;
		// find position
		int i = 0;
		while(currNode->keyArray[i] < entry.key && i < nodeOccupancy && currNode->pageNoArray[i + 1] != 0){
			i++;
		}
		if(currNode->pageNoArray[i] == 0){ // create leaf since there are no more positions
			Page* newPage;
			PageId newPageId;
			bufMgr->allocPage(file, newPageId, newPage);
			LeafNodeString* newNode = (LeafNodeString*) newPage;
			for(int j = 0; j < nodeOccupancy; j++){
				newNode->ridArray[j].page_number = 0;
			}
			newNode->ridArray[0] = entry.rid;
			newNode->rightSibPageNo = 0;
			currNode->pageNoArray[0] = newPageId;
			bufMgr->unPinPage(file, newPageId, true);
			bufMgr->unPinPage(file, pageNo, true);
			return; // done with inserting
		}

		bufMgr->unPinPage(file, pageNo, false);
		bool isLeaf = true;
		if(currNode->level == 1){
			isLeaf = true;
		}
		else{
			isLeaf = false;
		}
		insertString(entry, isLeaf, currNode->pageNoArray[i]);

		// TODO: check if node was split. if so, must insert *****************	
		// 


	}
	nodeOccupancy++;
	bufMgr->unPinPage(file, pageNo, true);*/
}

// -----------------------------------------------------------------------------
// BTreeIndex::startScan
// -----------------------------------------------------------------------------

//scan through leaf nodes until meets upper bound.

const void BTreeIndex::startScan(const void* lowValParm,
		const Operator lowOpParm,
		const void* highValParm,
		const Operator highOpParm)
{

	if((highOpParm != LTE && highOpParm != LT) || (lowOpParm != GTE && lowOpParm != GT)){
		throw BadOpcodesException();
	}

	lowOp = lowOpParm;
	highOp = highOpParm;

	if(lowValParm < highValParm){
		throw BadScanrangeException();
	}

	// checks if index scan has already started. Ends it if it has.
	if(scanExecuting){
		endScan();
	}

	scanExecuting = true;

	// if there are no nodes in the B+ tree
	if(rootPageNum == Page::INVALID_NUMBER){
		exit(1);
	}
	currentPageNum = rootPageNum;
	bufMgr->readPage(file, currentPageNum, currentPageData);

	if(attributeType == INTEGER){
		NonLeafNodeInt *currentNode = (NonLeafNodeInt *)currentPageData;
		lowValInt = *((int *)lowValParm);
		highValInt = *((int *)highValParm); 

		int i = 0;
		// loops until it finds the node that is one level above a leaf.
		while(currentNode->level != 1){
			i = 0;
			// finds beginning of range of nodes.
			while(currentNode->keyArray[i] < lowValInt && currentNode->pageNoArray[i + 1] != 0 && i < nodeOccupancy){
				i++;
			}
			// found first page:
			PageId nextNodePageNo = currentNode->pageNoArray[i];
			// reads page and saves it in currentPageData
			bufMgr->readPage(file, nextNodePageNo, currentPageData);
			bufMgr->unPinPage(file, currentPageNum, false);
			currentPageNum = nextNodePageNo;
			// moves to next node
			currentNode = (NonLeafNodeInt*) currentPageData;
		}
				
		// at level 1
		int j = 0;
		while(currentNode->keyArray[j] > lowValInt && currentNode->pageNoArray[j + 1] != 0 && j < nodeOccupancy){
			j++;
		}
		// found first page:
		PageId nextNodePageNo = currentNode->pageNoArray[j];
		// reads page and saves it in currentPageData
		bufMgr->readPage(file, nextNodePageNo, currentPageData);
		bufMgr->unPinPage(file, currentPageNum, false);
		currentPageNum = nextNodePageNo;
		// moves to next node
		nextEntry = 0; // at leaf!

	}
	else if(attributeType == DOUBLE){

		NonLeafNodeDouble *currentNode = (NonLeafNodeDouble *)currentPageData;
		lowValDouble = *((double *)lowValParm);
		highValDouble = *((double *)highValParm); 

		int i = 0;
		// loops until it finds the node that is one level above a leaf.
		while(currentNode->level != 1){
			i = 0;
			// finds beginning of range of nodes.
			while(currentNode->keyArray[i] < lowValDouble && currentNode->pageNoArray[i + 1] != 0 && i < nodeOccupancy){
				i++;
			}
			// found first page:
			PageId nextNodePageNo = currentNode->pageNoArray[i];
			// reads page and saves it in currentPageData
			bufMgr->readPage(file, nextNodePageNo, currentPageData);
			bufMgr->unPinPage(file, currentPageNum, false);
			currentPageNum = nextNodePageNo;
			// moves to next node
			currentNode = (NonLeafNodeDouble*) currentPageData;
		}
				
		// at level 1
		int j = 0;
		while(currentNode->keyArray[j] > lowValDouble && currentNode->pageNoArray[j + 1] != 0 && j < nodeOccupancy){
			j++;
		}
		// found first page:
		PageId nextNodePageNo = currentNode->pageNoArray[j];
		// reads page and saves it in currentPageData
		bufMgr->readPage(file, nextNodePageNo, currentPageData);
		bufMgr->unPinPage(file, currentPageNum, false);
		currentPageNum = nextNodePageNo;
		// moves to next node
		nextEntry = 0; // at leaf!

	}
	else if(attributeType == STRING){
		NonLeafNodeString *currentNode = (NonLeafNodeString *)currentPageData;
		lowValString = std::string((char *)lowValParm).substr(0,STRINGSIZE);
		highValString = std::string((char *)highValParm).substr(0,STRINGSIZE);

		int i = 0;
		// loops until it finds the node that is one level above a leaf.
		while(currentNode->level != 1){
			i = 0;
			// finds beginning of range of nodes.
			while(currentNode->keyArray[i] < lowValString && currentNode->pageNoArray[i + 1] != 0 && i < nodeOccupancy){
				i++;
			}
			// found first page:
			PageId nextNodePageNo = currentNode->pageNoArray[i];
			// reads page and saves it in currentPageData
			bufMgr->readPage(file, nextNodePageNo, currentPageData);
			bufMgr->unPinPage(file, currentPageNum, false);
			currentPageNum = nextNodePageNo;
			// moves to next node
			currentNode = (NonLeafNodeString*) currentPageData;
		}
				
		// at level 1
		int j = 0;
		while(currentNode->keyArray[j] > lowValString && currentNode->pageNoArray[j + 1] != 0 && j < nodeOccupancy){
			j++;
		}
		// found first page:
		PageId nextNodePageNo = currentNode->pageNoArray[j];
		// reads page and saves it in currentPageData
		bufMgr->readPage(file, nextNodePageNo, currentPageData);
		bufMgr->unPinPage(file, currentPageNum, false);
		currentPageNum = nextNodePageNo;
		// moves to next node
		nextEntry = 0; // at leaf!

	}
}

// -----------------------------------------------------------------------------
// BTreeIndex::scanNext
// -----------------------------------------------------------------------------

const void BTreeIndex::scanNext(RecordId& outRid)
{
	if(scanExecuting == false){
		throw ScanNotInitializedException();
	}
	bufMgr->readPage(file, currentPageNum, currentPageData);

	if(attributeType == INTEGER){
		bool cont = true;
		while(cont){
			LeafNodeInt *currentNode = (LeafNodeInt *)currentPageData;
			if((nextEntry >= nodeOccupancy) || (currentNode->ridArray[nextEntry].page_number == 0)){
				PageId nextPageNo = currentNode->rightSibPageNo;
				bufMgr->unPinPage(file, currentPageNum, false);
				if(nextPageNo == 0){ // scan is done:
					throw IndexScanCompletedException();
				}
				// scan is not done:
				currentPageNum = nextPageNo;
				bufMgr->readPage(file, currentPageNum, currentPageData);
				nextEntry = 0;
			}
			else if(((currentNode->keyArray[nextEntry] > lowValInt) && (lowOp == GT)) 
				|| ((currentNode->keyArray[nextEntry] <= lowValInt) && (lowOp == GTE))){
					nextEntry++;
			}
			else if(((currentNode->keyArray[nextEntry]) < highValInt && (highOp == LT)) 
				|| ((currentNode->keyArray[nextEntry] <= highValInt) && (highOp == LTE))){
				throw IndexScanCompletedException();
			}
			else{
				outRid = currentNode->ridArray[nextEntry];
				nextEntry++;
				cont = false;
			}
		}
	}
	else if(attributeType == DOUBLE){
		bool cont = true;
		while(cont){
			LeafNodeDouble *currentNode = (LeafNodeDouble *)currentPageData;
			if((nextEntry >= nodeOccupancy) || (currentNode->ridArray[nextEntry].page_number == 0)){
				PageId nextPageNo = currentNode->rightSibPageNo;
				bufMgr->unPinPage(file, currentPageNum, false);
				if(nextPageNo == 0){ // scan is done:
					throw IndexScanCompletedException();
				}
				// scan is not done:
				currentPageNum = nextPageNo;
				bufMgr->readPage(file, currentPageNum, currentPageData);
				nextEntry = 0;
			}
			else if(((currentNode->keyArray[nextEntry] > lowValDouble) && (lowOp == GT)) 
				|| ((currentNode->keyArray[nextEntry] <= lowValDouble) && (lowOp == GTE))){
					nextEntry++;
			}
			else if(((currentNode->keyArray[nextEntry]) < highValDouble && (highOp == LT)) 
				|| ((currentNode->keyArray[nextEntry] <= highValDouble) && (highOp == LTE))){
				throw IndexScanCompletedException();
			}
			else{
				outRid = currentNode->ridArray[nextEntry];
				nextEntry++;
				cont = false;
			}
		}
	}
	else if(attributeType == STRING){
		bool cont = true;
		while(cont){
			LeafNodeString *currentNode = (LeafNodeString *)currentPageData;
			if((nextEntry >= nodeOccupancy) || (currentNode->ridArray[nextEntry].page_number == 0)){
				PageId nextPageNo = currentNode->rightSibPageNo;
				bufMgr->unPinPage(file, currentPageNum, false);
				if(nextPageNo == 0){ // scan is done:
					throw IndexScanCompletedException();
				}
				// scan is not done:
				currentPageNum = nextPageNo;
				bufMgr->readPage(file, currentPageNum, currentPageData);
				nextEntry = 0;
			}
			else if(((currentNode->keyArray[nextEntry] > lowValString) && (lowOp == GT)) 
				|| ((currentNode->keyArray[nextEntry] <= lowValString) && (lowOp == GTE))){
					nextEntry++;
			}
			else if(((currentNode->keyArray[nextEntry]) < highValString && (highOp == LT)) 
				|| ((currentNode->keyArray[nextEntry] <= highValString) && (highOp == LTE))){
				throw IndexScanCompletedException();
			}
			else{
				outRid = currentNode->ridArray[nextEntry];
				nextEntry++;
				cont = false;
			}
		}
	}
}


// -----------------------------------------------------------------------------
// BTreeIndex::endScan
// -----------------------------------------------------------------------------
//
const void BTreeIndex::endScan()
{
	if(scanExecuting == false){
		throw ScanNotInitializedException();
	}
	scanExecuting = false;
	if(currentPageNum != 0){
		bufMgr->unPinPage(file, currentPageNum, false);
	}

}

}
