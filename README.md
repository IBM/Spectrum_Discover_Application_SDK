Application SDK
===================

The Application SDK is a set of libraries provided to aid with the development of applications that leverage
the IBM Spectrum Discover platform capabilities. An application in this context is described as a program that
can act upon the source data to extract relevant metadata or can modify the source data as instructed. Examples
of this are content inspection, anonymization, or data movement. When a policy is run against an application
the application receives instructions from the Spectrum Discover PolicyEngine (called 'action params') and a
list of documents to perform those actions on.

The application libraries provide functions to:
   * Initialize the application and register it with the Spectrum Discover host
   * Read and parse work messages from the PolicyEngine
   * Retrieve the contents of the documents via Spectrum Discover connections
   * Construct and send the results back to the PolicyEngine

The Application SDK provides abstracted methods to send and receive the relevant information for the work messages
and sending the results back to the PolicyEngine.

An example application using these libraries is also provided in this repository.

Building a python wheel
===================
You can create a local python wheel with the following command:
```
python3.6 setup.py sdist bdist_wheel --universal --dist=dist
```

Building a docker image
===================
Assuming that you have already created the wheel...
```
docker build .
docker image ls | head -n 2
```
