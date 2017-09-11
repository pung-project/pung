#include <iostream>
#include "pungPIR.hpp"

using namespace std;

PungPIRServer::
PungPIRServer(uint64_t len, char *stream, uint64_t nb_files, PIRParameters p) 
{
  params.alpha = p.alpha;
  params.d = p.d;
  params.n[0] = p.n[0];
  params.n[1] = p.n[1];
  params.n[2] = p.n[2];

  params.crypto_params = p.crypto_params; 

  crypto = HomomorphicCryptoFactory::getCryptoMethod(params.crypto_params);
  crypto->setandgetAbsBitPerCiphertext(params.n[0]);

  db = new DBArrayProcessor(len, stream, nb_files);
  //cout<<"Length of db "<<len<<endl;
  //cout<<"nb_files "<<nb_files<<endl;

  PIRReplyGenerator *r_generator = new PIRReplyGenerator(this->params, *crypto, db); 
  r_generator->setPirParams(this->params);
  imported_db = r_generator->importData(0, db->getmaxFileBytesize());
  //cout<<"db element size : "<<db->getmaxFileBytesize()<<endl;
  delete r_generator;
}

PungPIRServer::
~PungPIRServer()
{
  delete imported_db;
}

char* PungPIRServer::
processQuery(char*q, uint64_t len, uint64_t len_element, uint64_t *rlen, uint64_t *rlen_element)
{
  PIRReplyGenerator *r_generator = new PIRReplyGenerator(this->params, *crypto, db); 
  r_generator->setPirParams(this->params);

  for (unsigned int i=0; i<len; i+=len_element)
  {
    r_generator->pushQuery(&q[i]);
  }

  r_generator->generateReply(imported_db);

  char* reply_element;
  vector<char*> r;
  while (r_generator->popReply(&reply_element))
  {
    r.push_back(reply_element);
  }

  *rlen_element = r_generator->getReplyElementBytesize();
  *rlen = r.size() * *rlen_element;

  char *outptr, *result; 
  result = (char*)calloc(*rlen, sizeof(char));
  outptr = result;
  
  for(unsigned int i=0; i<r.size(); i++)
  {
    memcpy(outptr, r[i], *rlen_element);
    outptr += *rlen_element;
    free(r[i]);
  }

  r_generator->freeQueries();
  delete r_generator;

  return result;
}

// Pung PIR client-related classes and methods
PungPIRClient::
PungPIRClient(PIRParameters p, uint64_t recordSize) 
{
  params.alpha = p.alpha;
  params.d = p.d;
  params.n[0] = p.n[0];
  params.n[1] = p.n[1];
  params.n[2] = p.n[2];

  params.crypto_params = p.crypto_params;

  crypto = HomomorphicCryptoFactory::getCryptoMethod(params.crypto_params);
  crypto->setandgetAbsBitPerCiphertext(params.n[0]);
  this->maxFileBytesize = recordSize;
}

void PungPIRClient::
updateDBParams(PIRParameters p, uint64_t recordSize)
{
  params.alpha = p.alpha;
  params.d = p.d;
  params.n[0] = p.n[0];
  params.n[1] = p.n[1];
  params.n[2] = p.n[2];

  this->maxFileBytesize = recordSize;
  crypto->setandgetAbsBitPerCiphertext(params.n[0]);
}

PungPIRClient::
~PungPIRClient()
{
}

uint64_t PungPIRClient::
generateQuery(uint64_t chosen_idx, vector<char*>* q)
{
  PIRQueryGenerator *q_generator = new PIRQueryGenerator(params, *crypto);
  
  this->lastChosenIdx = chosen_idx;
  q_generator->generateQuery(chosen_idx/params.alpha);

  char* query_element;
  while (q_generator->popQuery(&query_element))
  {
    q->push_back(query_element);
  }

  uint64_t len = q_generator->getQueryElementBytesize();
  delete q_generator;
  return len;
}

char* PungPIRClient::
processReply(char* r, uint64_t len, uint64_t len_element, uint64_t *rlen)
{
  PIRReplyExtraction *r_extractor = new PIRReplyExtraction(params, *crypto);

  for (unsigned int i=0; i<len; i+=len_element)
  {
    //cout<<"Pushing encrypted element"<<endl;
    r_extractor->pushEncryptedReply(&r[i]);
  }

  r_extractor->extractReply(this->maxFileBytesize);

  char *outptr, *result, *tmp;
  result = (char*)calloc(r_extractor->getnbPlaintextReplies(this->maxFileBytesize)*r_extractor->getPlaintextReplyBytesize(), sizeof(char));
  outptr = result;

  int len_response = 0;

  while (r_extractor->popPlaintextResult(&tmp)) 
  {
    uint64_t plen = r_extractor->getPlaintextReplyBytesize();
    //cout<<"Plen is "<<plen<<endl;
    memcpy(outptr, tmp, plen); //r_extractor->getPlaintextReplyBytesize()); 
    outptr += plen; //r_extractor->getPlaintextReplyBytesize();
    free(tmp);
    len_response += plen; //r_extractor->getPlaintextReplyBytesize();
  }
  
  //cout<<"Length of response "<<len_response<<endl;
  //for (int i=0; i<len_response; i++)
  //  cout<<int(result[i]);

  //cout<<endl;

  outptr = (char*) calloc(maxFileBytesize, sizeof(char));
  uint64_t offset = this->lastChosenIdx % params.alpha;
  for (unsigned int i=0; i<maxFileBytesize; i++)
  {
    outptr[i] = result[offset*maxFileBytesize + i];
  }

  *rlen = this->maxFileBytesize;
  free(result);

  result = outptr;

  delete r_extractor;

  return result;
}

//--------------------------------------------------------------------------------
//
//// methods to be called from Rust

void* 
cpp_client_setup(uint64_t len_total_bytes, uint64_t num_db_entries, uint64_t alpha, uint64_t d)
{
 
  uint64_t max_entry_size_bytes = len_total_bytes/num_db_entries;
  
  uint64_t num_extra_entries = 0;

  if (num_db_entries % alpha != 0) {
    num_extra_entries = alpha - (num_db_entries % alpha);
  }

  //cout<<"Num extra entries on client: "<<num_extra_entries<<endl;
  DefaultPIRParams *params = new DefaultPIRParams(num_db_entries+num_extra_entries, alpha, d); 
  PungPIRClient *pir = new PungPIRClient(params->getParams(), max_entry_size_bytes);
  delete params;
  return (void*) pir;
}

uint64_t 
client_generate_query_internal(PungPIRClient *pir, uint64_t chosen_idx, vector<char*>* q)
{
  return pir->generateQuery(chosen_idx, q);
}

char* 
cpp_client_generate_query(void* pir, uint64_t chosen_idx, uint64_t* rlen_total_bytes, uint64_t* rnum_logical_entries)
{
  vector<char*> q;

  uint64_t rlen_element = ((PungPIRClient*) pir)->generateQuery(chosen_idx, &q);

  *rlen_total_bytes = rlen_element*q.size();
  *rnum_logical_entries = *rlen_total_bytes/rlen_element;

  char *outptr, *result; 
  result = (char*)calloc(*rlen_total_bytes, sizeof(char));
  outptr = result;

  for(unsigned int i=0; i < q.size(); i++)
  {
    memcpy(outptr, q[i], rlen_element);
    outptr += rlen_element;
    free(q[i]);
  }
  return result;
}

void 
cpp_client_update_db_params(void *pir, uint64_t len_db_total_bytes, uint64_t num_db_entries, uint64_t alpha, uint64_t d)
{
  uint64_t len_element = len_db_total_bytes/num_db_entries;
 
  uint64_t num_extra_entries = 0; 
  if (num_db_entries % alpha != 0) {
    num_extra_entries = alpha - (num_db_entries % alpha);
  }
  
  DefaultPIRParams *params = new DefaultPIRParams(num_db_entries+num_extra_entries, alpha, d); 
  ((PungPIRClient*)pir)->updateDBParams(params->getParams(), len_element);
}

char*
cpp_client_process_reply(void* pir, char* r, uint64_t len_total_bytes, uint64_t num_logical_entries, uint64_t* rlen_total_bytes)
{
  uint64_t len_element = len_total_bytes/num_logical_entries;
  return ((PungPIRClient*) pir)->processReply(r, len_total_bytes, len_element, rlen_total_bytes);
}

void 
cpp_client_free(void *pir)
{
  delete (PungPIRClient*) pir;
}

void 
cpp_buffer_free(char *buf)
{
  free(buf);
}

void* 
cpp_server_setup(uint64_t len_total_bytes, char *db, uint64_t num_logical_entries, uint64_t alpha, uint64_t d) 
{
  uint64_t max_entry_size_bytes = len_total_bytes/num_logical_entries;
  
  uint64_t num_extra_entries = 0;
  uint64_t num_extra_bytes = 0;

  char *padded_db;

  if (num_logical_entries % alpha != 0) {
    num_extra_entries = alpha - (num_logical_entries % alpha);
    num_extra_bytes = num_extra_entries*max_entry_size_bytes;
    padded_db = (char*) calloc(len_total_bytes+num_extra_bytes, sizeof(char));

    //cout<<"Num extra entries on server : "<<num_extra_entries<<endl;
    //cout<<"Num extra bytes on server : "<<num_extra_bytes<<endl;
    for (unsigned int i=0; i<len_total_bytes; i++) 
      padded_db[i] = db[i];
    for (unsigned int i=len_total_bytes; i<num_extra_bytes+len_total_bytes; i++)
      padded_db[i] = (char)1;
  
    //cout<<"padded db is "<<endl;
    /*for (int i=0; i<(num_extra_bytes+len_total_bytes)/max_entry_size_bytes; i++)
    {  
      cout<<"Entry: ";
      for (int j=0; j<max_entry_size_bytes; j++)
      cout<<int(padded_db[i*max_entry_size_bytes+j]);
      cout<<endl;
    }
      cout<<endl;*/

  } else {
    padded_db = db;
  }

  DefaultPIRParams *params = new DefaultPIRParams(num_logical_entries+num_extra_entries, alpha, d); 
  PungPIRServer *pir = new PungPIRServer(len_total_bytes+num_extra_bytes, padded_db, num_logical_entries+num_extra_entries, params->getParams());
  
  if (num_logical_entries % alpha != 0) {
    free(padded_db);
  }

  delete params;
  return (void*) pir;
}

char* 
cpp_server_process_query(void* pir, char* q, uint64_t len_total_bytes, uint64_t num_logical_entries, uint64_t* rlen_total_bytes, uint64_t* rnum_logical_entries)
{
  uint64_t len_element = len_total_bytes/num_logical_entries;
  uint64_t rlen_element;
  char *response = ((PungPIRServer*) pir)->processQuery(q, len_total_bytes, len_element, rlen_total_bytes, &rlen_element);
  *rnum_logical_entries = *rlen_total_bytes/rlen_element;
  return response;
}

void 
cpp_server_free(void *pir)
{
  delete (PungPIRServer*) pir;
}
