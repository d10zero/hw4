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
	//std::ostringstream idxStr;
	//idxStr << relationName << ’.’ << attrByteOffset;
	//std::string indexName = idxStr.str() ; // indexName is the name of the index file

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

		
		if(metaData->attrByteOffset != attrByteOffset
		   || metaData->attrType != attributeType
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
			NonLeafNodeInt *nonLeafNodeInt = (NonLeafNodeInt *) currPage;
			for(int i = 0; i < nodeOccupancy; i++){
				nonLeafNodeInt->pageNoArray[i] = 0;
			} 
			nonLeafNodeInt->level = 1;
		} else if(attributeType == DOUBLE){
			NonLeafNodeDouble *nonLeafNodeDouble = (NonLeafNodeDouble *) currPage;
			for(int i = 0; i < nodeOccupancy; i++){
				nonLeafNodeDouble->pageNoArray[i] = 0;
			} 
			nonLeafNodeDouble->level = 1;
		} else if (attributeType == STRING) {
			NonLeafNodeString *nonLeafNodeString = (NonLeafNodeString *) currPage;
			for(int i = 0; i < nodeOccupancy; i++){
				nonLeafNodeString->pageNoArray[i] = 0;
			} 
			nonLeafNodeString->level = 1;
		}
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
		PageId updatedId = 0;
		insertInteger(entry, false, rootPageNum, updatedId);
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
const void BTreeIndex::insertInteger(RIDKeyPair<int> entry, bool leaf, PageId pageNo, PageId updatedId) 
{

// TODO: (may need a different method for this or what is below) NEW ROOT NODE

	// for a non-leaf node
	if(!leaf){
		NonLeafNodeInt newNodeVal;
		insertNonLeafInteger(entry, newNodeVal, leaf, pageNo, updatedId);
	}
	// for a leaf node:
	else {
		LeafNodeInt newNodeVal;
		insertLeafInteger(entry, newNodeVal, leaf, pageNo, updatedId);

	}


}


// ----------------------------------------------------------------------------
//BTreeIndex::insertInteger
//
// ---------------------------------------------------------------------------
const void BTreeIndex::insertNonLeafInteger(RIDKeyPair<int> entry, NonLeafNodeInt newNodeVal, bool leaf, PageId pageNo, PageId updatedId) 
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
	// recursive call:
	insertInteger(entry, isLeaf, currentNode->pageNoArray[i], newNodeId);

	

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
const void BTreeIndex::insertLeafInteger(RIDKeyPair<int> entry, LeafNodeInt newNodeVal, bool leaf, PageId pageNo, PageId updatedId)
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
		// TODO: must split ***************
			// split with updatedId

	}
	leafOccupancy++;
	bufMgr->unPinPage(file, pageNo, true);



}
// ----------------------------------------------------------------------------
//BTreeIndex::insertDouble
//
// ---------------------------------------------------------------------------
const void BTreeIndex::insertDouble(RIDKeyPair<double> entry, bool leaf, PageId pageNo) 
{
	// for a leaf node:
	PageId* currPageNum;
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
			PageId newPageId;
			bufMgr->allocPage(file, newPageId, newPage);
			LeafNodeDouble* newNode = (LeafNodeDouble*) newPage;
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
				nextEntry;
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
				nextEntry;
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
