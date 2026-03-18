/***************** Transaction class **********************/
/*** Implements methods that handle Begin, Read, Write, ***/
/*** Abort, Commit operations of transactions. These    ***/
/*** methods are passed as parameters to threads        ***/
/*** spawned by Transaction manager class.              ***/
/**********************************************************/

/* Spring 2026: CSE 4331/5331 Project 2 : Tx Manager */

/* Required header files */
#include <stdio.h>
#include <stdlib.h>
#include <sys/signal.h>
#include "zgt_def.h"
#include "zgt_tm.h"
#include "zgt_extern.h"
#include <unistd.h>
#include <iostream>
#include <fstream>
#include <pthread.h>


extern void *start_operation(long, long);  //start an op with mutex lock and cond wait
extern void *finish_operation(long);        //finish an op with mutex unlock and con signal

extern void *do_commit_abort_operation(long, char);   //commit/abort based on char value
extern void *process_read_write_operation(long, long, int, char);

extern zgt_tm *ZGT_Sh;			// Transaction manager object

/* Transaction class constructor */
/* Initializes transaction id and status and thread id */
/* Input: Transaction id, status, thread id */

zgt_tx::zgt_tx( long tid, char Txstatus, char type, pthread_t thrid){
  this->lockmode = (char)' ';   // default
  this->Txtype = type;          // R = read only, W=Read/Write
  this->sgno =1;
  this->tid = tid;
  this->obno = -1;              // set it to a invalid value
  this->status = Txstatus;
  this->pid = thrid;
  this->head = NULL;
  this->nextr = NULL;
  this->semno = -1;             // init to an invalid sem value
}

/* Method used to obtain reference to a transaction node      */
/* Inputs the transaction id. Makes a linear scan over the    */
/* linked list of transaction nodes and returns the reference */
/* of the required node if found. Otherwise returns NULL      */

zgt_tx* get_tx(long tid1){  
  zgt_tx *txptr, *lastr1;
  
  if(ZGT_Sh->lastr != NULL){	// If the list is not empty
      lastr1 = ZGT_Sh->lastr;	// Initialize lastr1 to first node's ptr
      for  (txptr = lastr1; (txptr != NULL); txptr = txptr->nextr)
	    if (txptr->tid == tid1) 		// if required id is found									
	       return txptr; 
      return (NULL);			// if not found in list return NULL
   }
  return(NULL);				// if list is empty return NULL
}

/* Method that handles "BeginTx tid" in test file     */
/* Inputs a pointer to transaction id, obj pair as a struct. Creates a new  */
/* transaction node, initializes its data members and */
/* adds it to transaction list */

void *begintx(void *arg){
  //intialise a transaction object. Make sure it is 
  //done after acquiring the semaphore for the tm and making sure that 
  //the operation can proceed using the condition variable. When creating
  //the tx object, set the tx to TR_ACTIVE and obno to -1; there is no 
  //semno as yet as none is waiting on this tx.
  
  struct param *node = (struct param*)arg;// get tid and count
  start_operation(node->tid, node->count); 
    zgt_tx *tx = new zgt_tx(node->tid,TR_ACTIVE, node->Txtype, pthread_self());	// Create new tx node
 
    // Writes the Txtype to the file.
  
    zgt_p(0);				// Lock Tx manager; Add node to transaction list
  
    tx->nextr = ZGT_Sh->lastr;
    ZGT_Sh->lastr = tx;   
    zgt_v(0); 			// Release tx manager 
    fprintf(ZGT_Sh->logfile, "T%d\t%c\tBeginTx\n", node->tid, node->Txtype);	// Write log record and close
    fflush(ZGT_Sh->logfile);
    finish_operation(node->tid);
    free(node);//added em
    pthread_exit(NULL);				// thread exit 
}

/* Method to handle Readtx action in test file    */
/* Inputs a pointer to structure that contans     */
/* tx id and object no to read. Reads the object  */
/* if the object is not yet present in hash table */
/* or same tx holds a lock on it. Otherwise waits */
/* until the lock is released */

void *readtx(void *arg){
  //[SAteefa]
  struct param *node = (struct param*)arg;
  process_read_write_operation(node->tid, node->obno, node->count, 'S');
  free(node);
  pthread_exit(NULL);
}


void *writetx(void *arg){ //do the operations for writing; similar to readTx //emely
  struct param *node = (struct param*)arg;	/// gest tid, obno, and sequence count
  
  // do the operations for writing; similar to readTx.
////////
  // process the write operation
  process_read_write_operation(node->tid, node->obno, node->count, 'W'); //W will request x lock on obj
  
  free(node);//added em
  pthread_exit(NULL);   // thread exit
}

// common method to process read/write: Just a suggestion

void *process_read_write_operation(long tid, long obno, int count, char mode) //emely
{
    //ensures operations execute in order within the same transaction
    start_operation(tid, count);

    //  Get the transaction
    zgt_tx *tx = get_tx(tid); //get the trans obj from the trans list
    if (tx == NULL) {//if there arent any
        finish_operation(tid);
        return NULL;
    }

    //determine lock type //!!!shared locks allow concurrent reads but exclusive locks block all
     * other access
    char lockmode;
    if (mode == 'S') {
        lockmode = 'S';  // shared lock for read
    } else {
        lockmode = 'X';  // xclusive lock blocks others
    }

    // Try to acquire lock from lock mgr
    if (tx->set_lock(tid, 1, obno, count, lockmode) < 0) {//lock granted=0, will wait on semaphore/abort otherwise
        finish_operation(tid);
        return NULL;
    }

    // Do the actual read/write
    tx->perform_read_write_operation(tid, obno, mode);

    //Signals that operation is complet
    finish_operation(tid);// now next operation can proceed

    return NULL;
}

void *aborttx(void *arg) //ABORT operation
{

  //[SAteefa 3/16/2026]
  //convert argumnent to struct to get Tx info
  struct param *node = (struct param*)arg;// get tid and count  

  //ensure that operations in the same transation execute in order
  start_operation(node->tid, node->count); //blocks thread until all previous operations in the same transaction have completed (transaction consistency).
  //change transaction state to TR_ABORT, release all locks currently held by transactio , wake up any transactions waiting for locks, and log the abort operation.
  do_commit_abort_operation(node->tid, TR_ABORT); 
  //signal that the transaction finished. this wakes the next pending operation in the same transaction seqeunce.
  finish_operation(node->tid);
  free(node);//added em
  pthread_exit(NULL);			// thread exit
}

void *committx(void *arg) //emely
{
 
    //remove the locks/objects before committing 
  struct param *node = (struct param*)arg;// get tid and count

    ///makes sure prev ops of this trans completed
  start_operation(node->tid, node->count);
    
  do_commit_abort_operation(node->tid, TR_END); // status to TR_END// releases all locks//

    // log commit op
  fflush(ZGT_Sh->logfile); //

    // finish operation
  finish_operation(node->tid);// signals that this operation is completeand lets the nextcsame trans go next
  free(node);
  pthread_exit(NULL);			// thread exit
}

//suggestion as they are very similar

// called from commit/abort with appropriate parameter to do the actual
// operation. Make sure you give error messages if you are trying to
// commit/abort a non-existent tx

void *do_commit_abort_operation(long t, char status){

  //WMorokoshi[03/15/2026]
 zgt_tx *tx;
  long    myTid;    // save tid for signaling after sem0 released

  // Acquire TM lock to safely access and modify shared structures
  zgt_p(0);
  tx = get_tx(t);

  if (tx == NULL){
    fprintf(ZGT_Sh->logfile,
            "T%d\tError: Trying to %s a non-existent transaction\n",
            (int)t, (status == TR_END) ? "Commit" : "Abort");
    fflush(ZGT_Sh->logfile);
    zgt_v(0);
    return NULL;
  }

  // Record status change
  tx->status = status;
  myTid = tx->tid;   // save before remove

  if (status == TR_END){
    fprintf(ZGT_Sh->logfile, "T%d\t\tCommitTx\t", (int)t);
  } else {
    fprintf(ZGT_Sh->logfile, "T%d\t\tAbortTx\t", (int)t);
  }
  fflush(ZGT_Sh->logfile);

  // Free all locks held by this transaction; logs object values
  tx->free_locks();

  // Remove tx node from TM list
  tx->remove_tx();

  zgt_v(0);

  for (int i = 0; i < MAX_TRANSACTIONS; i++){
    zgt_v(myTid);
  }

  return NULL;
}

int zgt_tx::remove_tx()
{
    zgt_tx *curr = ZGT_Sh->lastr;
    zgt_tx *prev = NULL;

    while (curr != NULL) {
        if (curr->tid == this->tid) {
            if (prev == NULL)
                ZGT_Sh->lastr = curr->nextr;   // removing head
            else
                prev->nextr = curr->nextr;     // removing middle/tail
            return 0;
        }
        prev = curr;
        curr = curr->nextr;
    }

    fprintf(ZGT_Sh->logfile, "Trying to Remove a Tx:%d that does not exist\n", this->tid);
    fflush(ZGT_Sh->logfile);
    printf("Trying to Remove a Tx:%d that does not exist\n", this->tid);
    fflush(stdout);
    return -1;
}

/* this method sets lock on objno1 with lockmode1 for a tx*/

int zgt_tx::set_lock(long tid1, long sgno1, long obno1, int count, char lockmode1){
  //if the thread has to wait, block the thread on a semaphore from the
  //sempool in the transaction manager. Set the appropriate parameters in the
  //transaction list if waiting.
  //if successful  return(0); else -1

  //[SAteefa 3/16/2026]
  while(1)
  { 
    zgt_p(0); //lock transaction manager/lock table
  
  zgt_tx *tx = get_tx(tid1); //find transaction
  if(tx == NULL)             //transaction missing?
  {
    fprintf(ZGT_Sh->logfile, "Lock request by non-existent Tx %ld\n", tid1); //log error in system logfile
    fflush(ZGT_Sh->logfile); //ensure the log output is immediately written
    zgt_v(0); //release transaction manager semphore
    return -1; //fail transaction can't proceed :(
  }

  zgt_hlink *held = ZGT_Ht->findt(tid1, sgno1, obno1);
  if (held != NULL)
  {
    if (held->lockmode == 'X' || held->lockmode == lockmode1) {
        tx->status = TR_ACTIVE;
        tx->obno = -1;
        tx->lockmode = ' ';
        zgt_v(0);
        return 0;
    }

    // upgrade S -> X only if no other transaction holds this object
    if (held->lockmode == 'S' && lockmode1 == 'X') {
        bool otherHolderExists = false;
        long holderTid = -1;

        for (zgt_hlink *h = ZGT_Sh->head[((sgno1 + 1) * obno1) & (ZGT_DEFAULT_HASH_TABLE_SIZE - 1)];
             h != NULL;
             h = h->next)
        {
            if (h->sgno != sgno1 || h->obno != obno1) continue;
            if (h->tid == tid1) continue;

            otherHolderExists = true;
            holderTid = h->tid;
            break;
        }

        if (!otherHolderExists) {
            held->lockmode = 'X';
            tx->status = TR_ACTIVE;
            tx->obno = -1;
            tx->lockmode = ' ';
            zgt_v(0);
            return 0;
        }

        // otherwise do not return here; let the normal wait logic handle it
    }
  }

  //assume initially that the lock can be granted
  bool canGrant = true;
  //variable to store transaction that is currently blocking us
  long holderTid = -1;
  //transverse the lock table bucket corresponding to this object
  for(zgt_hlink *h = ZGT_Sh->head[((sgno1 + 1) * obno1) & (ZGT_DEFAULT_HASH_TABLE_SIZE -1)]; h!=NULL; h = h->next)
  {
    if(h->sgno != sgno1 || h->obno != obno1) continue;  //skip bc it does not correspond to same segment/object
    if(h->tid == tid1) continue; //skip entries that belong to the same transaction
    //zgt_tx *holderTx = get_tx(h->tid); //retrieve the transaction that currently holds the lock
    
    //if we are requesting an exclusive (write) lock
    if(lockmode1 == 'X')
    {
        //write lock conflict with any existing lock
        canGrant = false;
        //record the block transaction ID
        holderTid = h->tid;

        //stop scanning since a conflict was found
        break;
    }

    //if we are requesting a shared (read) lock
    if(lockmode1 == 'S')
    {
      //if existing holder is write transaction, this is a conflict
      if(h->lockmode == 'X')
      {
        //mark that lock cannot be granted immediately
        canGrant = false;
        //record which transaction is blocking us
        holderTid = h->tid;
        break; //exit loop since a conflicting holder was found
      }
    }
  }

  //if no conflict from the above were found, the lock can be given
  if(canGrant)
  {
    //attempt to add the lock to the lock table
    if(ZGT_Ht->add(tx, sgno1, obno1, lockmode1) < 0)
    {
      //log an error if the lock table insertion fails
      fprintf(ZGT_Sh->logfile, "Could not add lock for T%ld on O%ld\n", tid1, obno1);
      //flush the logfile to ensure the message is written
      fflush(ZGT_Sh->logfile);
      //release the transaction manager semaphore
      zgt_v(0);

      //return failure due to lock table insertion error
      return -1;
    }

      //update transaction state to active since the lock is now granted 
      tx->status = TR_ACTIVE;
      // clear the waiting object field
      tx->obno = -1;
      // clear the waiting lock mode
      tx->lockmode = ' ';
      // release the transaction manager semaphore
      zgt_v(0);
      // return success because the lock has been successfully granted
      return 0;
  }
   // if the lock could not be granted, the transaction must wait
    tx->status = TR_WAIT;
    tx->obno = obno1;
    tx->lockmode = lockmode1;

    fprintf(ZGT_Sh->logfile,
            "T%-2ld\t\t%-8s\t%3ld:%-3c:%6d\t\t%-9s\t%-10s\tW for T%ld\n",
            tid1,
            (lockmode1 == 'X') ? "WriteTx" : "ReadTx",
            obno1,
            (lockmode1 == 'X') ? 'X' : 'S',
            ZGT_Sh->optime[tid1],
            (lockmode1 == 'X') ? "WriteLock" : "ReadLock",
            "NotGranted",
            holderTid);
    fflush(ZGT_Sh->logfile);

    // associate the blocking transaction's semaphore with this transaction
    setTx_semno(holderTid, (int)holderTid);

    // release the transaction manager semaphore before going to sleep
    zgt_v(0);

    // block the current thread on the semaphore of the transaction holding the lock
    zgt_p((int)holderTid);

    // after waking up, retrieve the transaction again to verify its status
    tx = get_tx(tid1);

    // if the transaction no longer exists, return failure
    if (tx == NULL) return -1;

    // if the transaction was aborted while waiting, stop execution
    if (tx->status == TR_ABORT) return -1;

    // otherwise, retry acquiring the lock from the beginning of the loop
  } 
}

int zgt_tx::free_locks()
{
  
  // this part frees all locks owned by the transaction
  // that is, remove the objects from the hash table
  // and release all Tx's waiting on this Tx

  zgt_hlink* temp = head;  //first obj of tx
  
  for(;temp != NULL;temp = temp->nextp){	// SCAN Tx obj list

      fprintf(ZGT_Sh->logfile, "%d : %d, ", temp->obno, ZGT_Sh->objarray[temp->obno]->value);
      fflush(ZGT_Sh->logfile);
      
      if (ZGT_Ht->remove(this,1,(long)temp->obno) == 1){
	   printf(":::ERROR:node with tid:%d and onjno:%d was not found for deleting", this->tid, temp->obno);		// Release from hash table
	   fflush(stdout);
      }
      else {
#ifdef TX_DEBUG
	   printf("\n:::Hash node with Tid:%d, obno:%d lockmode:%c removed\n",
                            temp->tid, temp->obno, temp->lockmode);
	   fflush(stdout);
#endif
      }
    }
  fprintf(ZGT_Sh->logfile, "\n");
  fflush(ZGT_Sh->logfile);
  
  return 0;
}		

// CURRENTLY Not USED
// USED to COMMIT
// remove the transaction and free all associate dobjects. For the time being
// this can be used for commit of the transaction.

int zgt_tx::end_tx()  
{
  zgt_tx *linktx, *prevp;
  
  // USED to COMMIT 
  //remove the transaction and free all associate dobjects. For the time being 
  //this can be used for commit of the transaction.
  
  linktx = prevp = ZGT_Sh->lastr;
  
  while (linktx){
    if (linktx->tid  == this->tid) break;
    prevp  = linktx;
    linktx = linktx->nextr;
  }
  if (linktx == NULL) {
    printf("\ncannot remove a Tx node; error\n");
    fflush(stdout);
    return 1;
  }
  if (linktx == ZGT_Sh->lastr) ZGT_Sh->lastr = linktx->nextr;
  else {
    prevp = ZGT_Sh->lastr;
    while (prevp->nextr != linktx) prevp = prevp->nextr;
    prevp->nextr = linktx->nextr;    
  }
  return 0; //added em
}

// currently not used
int zgt_tx::cleanup()
{
  return 0;
  
}

// routine to print the tx list
// TX_DEBUG should be defined in the Makefile to print
void zgt_tx::print_tm(){
  
  zgt_tx *txptr;
  
#ifdef TX_DEBUG
  printf("printing the tx  list \n");
  printf("Tid\tTxType\tThrid\t\tobjno\tlock\tstatus\tsemno\n");
  fflush(stdout);
#endif
  txptr=ZGT_Sh->lastr;
  while (txptr != NULL) {
#ifdef TX_DEBUG
    printf("%d\t%c\t%d\t%d\t%c\t%c\t%d\n", txptr->tid, txptr->Txtype, txptr->pid, txptr->obno, txptr->lockmode, txptr->status, txptr->semno);
    fflush(stdout);
#endif
    txptr = txptr->nextr;
  }
  fflush(stdout);
}

//need to be called for printing
void zgt_tx::print_wait(){

  //route for printing for debugging
  
  printf("\n    SGNO        TxType       OBNO        TID        PID         SEMNO   L\n");
  printf("\n");
}

void zgt_tx::print_lock(){
  //routine for printing for debugging
  
  printf("\n    SGNO        OBNO        TID        PID   L\n");
  printf("\n");
  
}

// routine to perform the actual read/write operation as described the project description
// based  on the lockmode

void zgt_tx::perform_read_write_operation(long tid,long obno, char lockmode){
  
 //WMorokoshi[03/15/2026]

 int optime = ZGT_Sh->optime[tid];

  if (lockmode == 'S'){
    // Read operation: decrement value by 4
    ZGT_Sh->objarray[obno]->value -= 4;

    fprintf(ZGT_Sh->logfile,
            "T%-2d\t\t%-8s\t%3d:%-3d:%-6d\t\t%-9s\t%-7s\t\t%c\n",
            (int)tid,
            "ReadTx",
            (int)obno, 
            ZGT_Sh->objarray[obno]->value, 
            optime,
            "ReadLock",
            "Granted",
            this->status);
    fflush(ZGT_Sh->logfile);
  }
  else {
    // Write operation: increment value by 7
    ZGT_Sh->objarray[obno]->value += 7;

    fprintf(ZGT_Sh->logfile,
            "T%-2d\t\t%-8s\t%3d:%-3d:%-6d\t\t%-9s\t%-7s\t\t%c\n",
            (int)tid,
            "WriteTx",
            (int)obno, 
            ZGT_Sh->objarray[obno]->value, 
            optime,
            "WriteLock",
            "Granted",
            this->status);
    fflush(ZGT_Sh->logfile);
  }

  // Sleep for the operation time (microseconds)
  usleep(optime);

}

// routine that sets the semno in the Tx when another tx waits on it.
// the same number is the same as the tx number on which a Tx is waiting
int zgt_tx::setTx_semno(long tid, int semno){
  zgt_tx *txptr;
  
  txptr = get_tx(tid);
  if (txptr == NULL){
    printf("\n:::ERROR:Txid %d wants to wait on sem:%d of tid:%ld which does not exist\n", this->tid, semno, tid);
    fflush(stdout);
    exit(1);
  }
  if ((txptr->semno == -1)|| (txptr->semno == semno)){  //just to be safe
    txptr->semno = semno;
    return(0);
  }
  else if (txptr->semno != semno){
#ifdef TX_DEBUG
    printf(":::ERROR Trying to wait on sem:%d, but on Tx:%ld\n", semno, txptr->tid);
    fflush(stdout);
#endif
    exit(1);
  }
  return(0);
}

void *start_operation(long tid, long count){
  
  pthread_mutex_lock(&ZGT_Sh->mutexpool[tid]);	// Lock mutex[t] to make other
  // threads of same transaction to wait
  
  while(ZGT_Sh->condset[tid] != count)		// wait if condset[t] is != count
    pthread_cond_wait(&ZGT_Sh->condpool[tid],&ZGT_Sh->mutexpool[tid]);
  
  return NULL; //added em
}

// Otherside of teh start operation;
// signals the conditional broadcast

void *finish_operation(long tid){
  ZGT_Sh->condset[tid]--;	// decr condset[tid] for allowing the next op
  pthread_cond_broadcast(&ZGT_Sh->condpool[tid]);// other waiting threads of same tx
  pthread_mutex_unlock(&ZGT_Sh->mutexpool[tid]); 
  return NULL; //added em
}


