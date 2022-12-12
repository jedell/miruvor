# Measure the throughput of the distributed system

# Make x number of curl requests with the command curl -X POST 127.0.0.1:400{1-4}/write/{key}/{value}
# Randomly assign the port number with each request between 4001 and 4004
# assign key and value based on index of the request
# Measure the time it takes to complete each request and record
# Calculate the average time it takes to complete each request
# Calculate the throughput of the system

import requests
import random
import time
import sys

def main():
    # parse command line arguments for x
    x = int(sys.argv[1])
    # initialize variables
    total_time = 0
    url = "127.0.0.1"
    # make x number of requests
    for i in range(x):
        # randomly assign port number
        port = 4001 # random.randint(4001, 4004)
        # randomly assign key and value
        key = i
        value = i
        # make request
        start = time.time()
        # make request and await response
        response = requests.post(f"http://{url}:{port}/write/{key}/{value}", data=None,
                                 headers=None,
                                 )

        # check if request was successful
        if response.status_code != 200:
            print("Error: Request was not successful")
            print(f"Status code: {response.status_code}")
            print(f"Response: {response.text}")
            sys.exit(1)

        print(f"Response: {response.text}")

        end = time.time()
        # calculate time it took to complete request
        total_time += end - start
    # calculate average time it takes to complete a request
    avg_time = total_time / x
    # calculate throughput
    throughput = x / total_time
    # print results
    print("\n # Metrics # \n")
    print(f"Total time: {total_time}")
    print(f"Average time: {avg_time}")
    print(f"Throughput: {throughput}")


main()

# run main over 10 iterations
