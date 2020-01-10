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
If you have local modifications to the SDK that you would like to send via a Pull Request (PR), you can test your build
locally by creating a python wheel.
You can create a local python wheel with the following command:
```
python3.6 setup.py sdist bdist_wheel --universal --dist=dist
```

Building a docker image
===================
Assuming that you have already created the wheel, you can modify the Dockerfile to `COPY` the wheel and install.
An example modification of the Dockerfile would be:

`COPY dist/ibm_spectrum_discover_application_sdk-0.0.X-py2.py3-none-any.whl /` (above the existing RUN command)

and

`RUN python3 -m pip install dist/ibm_spectrum_discover_application_sdk-0.0.X-py2.py3-none-any.whl` (below existing RUN command)

where `X` in both cases is an incremented version number specified in the setup.py file. Remove these lines when submitting a PR.
Once accepted, the file will be grabbed automatically from the pypi repository.

You can then run the following command to create the docker image.
```
docker build .
docker image ls | head -n 2
```

You can now use this docker image as a template for containerizing the Example Application from:
`https://github.com/IBM/Spectrum_Discover_Example_Application`.

In the Spectrum_Discover_Example_Application/Dockerfile, you can change the `FROM` to import your container ID for testing purposes.