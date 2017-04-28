/*
 * UCLA CS133 - Parallel and Distributed Programming
 *
 * This program was used to empirically test whether it's better to schedule
 * jobs of heterogenous length in increasing or decreasing order of job length.
 *
 * My tests show that scheduling jobs in decreasing order of job length is
 * almost always faster, as intuition suggests.
 *
 * I was unable to formalize the problem in a mathematically satisfying way, so
 * I went with this approach. I initially planned on testing this with OpenMP
 * since that's one of the APIs we're learning in class, but decided to go with
 * a simulation-based approach to avoid any possible influence from OpenMP /
 * Linux sleep overheads.
 *
 * This program has no external dependencies.
 */

#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include <time.h>

// comparator for quicksort, sorts into ascending order
int increasing_compare(const void *a, const void *b)
{
  int int_a = * ( (int*) a );
  int int_b = * ( (int*) b );
  if ( int_a == int_b ) return 0;
  else if ( int_a < int_b ) return -1;
  else return 1;
}

// comparator for quicksort, sorts into decreasing order
int decreasing_compare(const void *a, const void *b)
{
  int int_a = * ( (int*) a );
  int int_b = * ( (int*) b );
  if ( int_a == int_b ) return 0;
  else if ( int_a < int_b ) return 1;
  else return -1;
}

// return array of length n_delays populated with random numbers
int* get_rand_delays(unsigned int n_delays, int max_delay,
    int (*compare)(const void *a, const void *b))
{
  int *delays = (int*) malloc(sizeof(int) * n_delays);
  if (delays == NULL) {
    fprintf(stderr, "Could not allocate delay array.\n");
    return NULL;
  }
  unsigned int i;
  for (i = 0; i < n_delays; i++) {
    delays[i] = (rand() % max_delay) + 1;
  }
  qsort(delays, (size_t) n_delays, sizeof(int), compare);
  return delays;
}

// simulate execution of jobs on num_processors
int run_simulation(int num_processors, int num_jobs, int *job_delays)
{
  int i = 0, j, time_elapsed = 0;
  int *processors = (int*) malloc(sizeof(int) * num_processors);
  if (processors == NULL) {
    fprintf(stderr, "Could not allocate processor array.\n");
    return -1;
  }
  bzero(processors, sizeof(int) * num_processors);
  int jobs_running = 1;
  while (jobs_running) {
    jobs_running = 0;
    // re-schedule idle processors
    for (j = 0; j < num_processors; j++) {
      if (processors[j] == 0 && i < num_jobs) {
        processors[j] = job_delays[i];
        i++;
        jobs_running = 1;
      } else if (processors[j] != 0) {
        jobs_running = 1;
      }
    }
    // decrement time remaining on all jobs by 1
    for (j = 0; j < num_processors; j++) {
      if (processors[j] != 0) {
        processors[j]--;
      }
    }
    time_elapsed++;
  }
  free(processors);
  return time_elapsed;
}

void test()
{
  // Test globals
  time_t rand_seed = time(NULL);

  // TEST 1: Check that get_rand_delays is deterministic when rand() is
  // reseeded
  {
    srand(rand_seed);
    int *a = get_rand_delays(100, 10000, increasing_compare);
    srand(rand_seed);
    int *b = get_rand_delays(100, 10000, increasing_compare);
    assert(memcmp(a, b, sizeof(int) * 100) == 0);
    free(a);
    free(b);
  }

  // TEST 2: Check that get_rand_delays is sorted in proper order, and that inc
  // == reverse(dec)
  {
    int i;
    srand(rand_seed);
    int *inc = get_rand_delays(100, 10000, increasing_compare);
    for (i = 1; i < 100; i++) {
      assert(inc[i-1] <= inc[i]);
    }
    srand(rand_seed);
    int *dec = get_rand_delays(100, 10000, decreasing_compare);
    for (i = 1; i < 100; i++) {
      assert(dec[i-1] >= dec[i]);
    }
    for (i = 0; i < 100; i++) {
      assert(inc[i] == dec[100-i-1]);
    }
  }

  printf("All tests passed.\n");
}

int main()
{
  test();

  int i, j, num_processors, num_jobs, max_delay;
  int num_trials = 100; // 100 trials per processor/job configuration to reduce
      // variance from get_rand_delays
  int *delays;
  srand(time(NULL));
  unsigned int rand_seed;

  // experimental results will be logged to a csv file
  FILE *result_file = fopen("results.csv", "w");
  if (result_file == NULL) {
    fprintf(stderr, "Could not open results.csv.\n");
    return -1;
  }

  fprintf(result_file, "processors,jobs,max_delay,avg_total_work," \
    "avg_inc_runtime,avg_dec_runtime\n");

  // 4, 8, 16 and 32 processors
  for (num_processors = 2; num_processors <= 64; num_processors *= 2) {
    for (num_jobs = 10; num_jobs <= 1000; num_jobs *= 10) {
      if (num_jobs <= num_processors) { // no scheduling would take place
        continue;
      }
      // max_delay is the longest unitless time a job can take. minimal delay
      // is implicitly 1
      for (max_delay = 100; max_delay <= 10000; max_delay *= 10) {
        unsigned long long inc_time_acc = 0, dec_time_acc = 0;
        unsigned long long delay_sum_acc = 0;
        // repeat trial with different random numbers to reduce variance
        for (i = 0; i < num_trials; i++) {
          rand_seed = rand();

          // increasing job length
          srand(rand_seed);
          delays = get_rand_delays(num_jobs, max_delay, increasing_compare);
          for (j = 0; j < num_jobs; j++) {
            delay_sum_acc += delays[j];
          }
          inc_time_acc += run_simulation(num_processors, num_jobs, delays);
          free(delays);

          // decreasing job length
          srand(rand_seed);
          delays = get_rand_delays(num_jobs, max_delay, decreasing_compare);
          dec_time_acc += run_simulation(num_processors, num_jobs, delays);
          free(delays);
        }
        // dump results to csv
        fprintf(result_file, "%d,%d,%d,%llu,%llu,%llu\n", num_processors,
            num_jobs, max_delay, delay_sum_acc / num_trials,
            inc_time_acc / num_trials, dec_time_acc / num_trials);
      }
    }
  }

  fclose(result_file);

  return 0;
}
