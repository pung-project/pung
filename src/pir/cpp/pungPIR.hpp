// This implements a shim layer that allows our Rust code to interface with the
// new functionality that we added to XPIR (the ability to work on a byte buffer 
// that is passed in rather than a file that has been stored in disk).

#ifndef PUNG_PIR_H
#define PUNG_PIR_H

#include <iostream>
#include "libpir.hpp"
#include "apps/server/DBArrayProcessor.hpp"
#include <math.h>

using namespace std;

class DefaultPIRParams {
  private:
    PIRParameters params; 

  public:
    DefaultPIRParams(uint64_t num_db_entries) 
    {
      params.alpha = 1; 
      params.d = 1; 
      params.n[0] = num_db_entries; 
      params.crypto_params = "LWE:80:1024:60"; 
    }
    
    void Factorize(uint64_t X, uint64_t *m, uint64_t *n)
    {
      uint64_t N = ceil(sqrt(X));
      uint64_t M; 
      while (1) {
        if (X % N == 0) 
        {
          M = X/N;
          *m = M;
          *n = N;
          if (X != M * N) {
            cout<<M<<"\t"<<N<<endl;
            cout<<"Could not factor"<<endl;
            exit(1);
          }
          return;
        } 
        else
        {
          N = N + 1;
        }
      }
    
      if (*m < *n) {
        uint64_t temp = *m;
        *m = *n;
        *n = temp;
      }
    }

    DefaultPIRParams(uint64_t num_db_entries, uint64_t alpha, uint64_t d) 
    {
      params.alpha = alpha;
      params.d = d; 

      
      if (d == 1)
      {
        params.n[0] = num_db_entries;
      } 
      else if (d == 2)
      {
        uint64_t X = num_db_entries;
        uint64_t m, n;
       
        if (num_db_entries % alpha != 0) {
          cout<<"Number of entries is not a muliple of alpha"<<endl;
          exit(1);
        }

        X = num_db_entries/alpha;
        
        Factorize(X, &m, &n);

        params.alpha = alpha;
        params.n[0] = (int)m;
        params.n[1] = (int)n;
//        cout<<"Alpha = "<<alpha<<" n1 = "<<m<<" n2 = "<<n<<endl;
      }
      else 
      {
        cout<<"Incorrect depth for recursion"<<endl;
        exit(1);
      }

      /*if (d == 3)
      {
        if ((int)cbrt(num_db_entries) * (int)cbrt(num_db_entries) * (int)cbrt(num_db_entries) != num_db_entries)
        {
          cout<<"Could not do recursion"<<endl;
          exit(1);
        }
        params.n[0] = (int)cbrt(num_db_entries);
        params.n[1] = (int)cbrt(num_db_entries);
        params.n[2] = (int)cbrt(num_db_entries);
      }
      else
      {
        cout<<"Incorrect depth for recursion"<<endl;
        exit(1);
      }*/

      params.crypto_params = "LWE:80:1024:60"; 
    }

    PIRParameters getParams()
    {
      return params;
    }
};

class PungPIRServer {

  private:
    HomomorphicCrypto *crypto;
    imported_database *imported_db;
    DBArrayProcessor *db;
    PIRParameters params;

  public:  
    PungPIRServer(uint64_t, char*, uint64_t, PIRParameters);
    ~PungPIRServer();
    char* processQuery(char*, uint64_t len, uint64_t len_element, uint64_t *rlen, uint64_t *rlen_element);
};

// Pung PIR client-related classes and methods
//
//
class PungPIRClient 
{
  private:
    HomomorphicCrypto *crypto;
    uint64_t maxFileBytesize;
    PIRParameters params;
    uint64_t lastChosenIdx;

  public:  
    PungPIRClient(PIRParameters, uint64_t);
    ~PungPIRClient();
    void updateDBParams(PIRParameters p, uint64_t);
    uint64_t generateQuery(uint64_t, vector<char*>*);
    char* processReply(char* r, uint64_t len, uint64_t len_element, uint64_t *rlen);
};


uint64_t client_generate_query_internal(PungPIRClient *pir, uint64_t chosen_idx, vector<char*>* q);

extern "C" {

  void* cpp_server_setup(uint64_t len_db_total_bytes, char *db, uint64_t num_db_entries, uint64_t alpha, uint64_t d); 
  void* cpp_client_setup(uint64_t len_db_total_bytes, uint64_t num_db_entries, uint64_t alpha, uint64_t d);

  void cpp_server_free(void* pir);
  void cpp_client_free(void* pir);
  void cpp_buffer_free(char* buf);

  char* cpp_client_generate_query(void* pir, uint64_t chosen_idx, uint64_t* rlen_query_total_bytes, uint64_t* rnum_query_slots);
  char* cpp_server_process_query(void* pir, char* q, uint64_t len_query_total_bytes, uint64_t num_query_slots, uint64_t* rlen_response_total_bytes, uint64_t* rnum_response_slots);
  char* cpp_client_process_reply(void* pir, char* r, uint64_t len_response_total_bytes, uint64_t num_response_slots, uint64_t* rlen_answer_total_bytes);
  void cpp_client_update_db_params(void* pir, uint64_t len_db_total_bytes, uint64_t num_db_entries, uint64_t alpha, uint64_t d);
}
#endif
