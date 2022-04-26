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
   * Retrieve the contents of the documents via Spectrum Discover/COS/NFS/SMB connections
   * Construct and send the results back to the PolicyEngine with associated tags

The Application SDK provides abstracted methods to send and receive the relevant information for the work messages
and sending the results back to the PolicyEngine.

An example application using these libraries is also provided in Spectrum_Discover_Example_Application repository.

Building a python wheel
===================
If you have local modifications to the SDK that you would like to send via a Pull Request (PR), you can test your build
locally by creating a python wheel. If you are not making changes to the SDK, there is no need to run
the below commands and you can use the currently published SDK on pypi.
You can create a local python wheel with the following command:

```
Edit the setup.py to increment the version number
 $ python3 -m pip install wheel
 $ python3 setup.py sdist bdist_wheel --universal --dist=dist
Edit the Dockerfile to increment the patch arg version
```

Building a docker image
===================
This dockerfile has been switched to use `buildah`.

buildah has the benefit of creating a mount while building so you no longer need to copy temp files into the build which increases the layer size.
This also assumes that you have a ubi8_packages.tar.gz file that contains rpms that are not available in the ubi repos. In our case we are using rpms from centos 8 as you cannot distribute non-ubi redhat rpms.
Read the [Red Hat UBI FAQ](https://developers.redhat.com/articles/ubi-faq) for more information.

The ubi8_packages.tar.gz file needs to contain the rpms and relevant dependencies for things like nfs-utils and cifs-utils. Before packaging up the rpms, you can issue a `createrepo .` within the folder to generate the repodata.
The tar.gz also needs to include a file to setup the repo.
In our case we called it `ubi8_local.repo` and the contents are:
```
[ubi-8-local]
baseurl = file:///install_media/ubi8_packages/
enabled = 1
gpgcheck = 0
name = Packages required for updating ubi8 images
```

Assuming that you have already created the wheel, you can then create the SDK image:
```
mkdir -p install_media
cp ubi8_packages.tar.gz install_media
cp dist/ibm_spectrum_discover_application_sdk-*-py2.py3-none-any.whl install_media
sudo buildah bud -f Dockerfile -t docker-daemon:ibmcom/spectrum-discover-application-sdk:latest -v $(pwd)/install_media:/install_media/ .
```

You can now use this docker image as a template for containerizing the [Example Application](https://github.com/IBM/Spectrum_Discover_Example_Application).