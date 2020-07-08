#!/usr/bin/python -W ignore
########################################################## {COPYRIGHT-TOP} ###
# Licensed Materials - Property of IBM
# 5737-I32
#
# (C) Copyright IBM Corp. 2019
#
# US Government Users Restricted Rights - Use, duplication, or
# disclosure restricted by GSA ADP Schedule Contract with IBM Corp.
########################################################## {COPYRIGHT-END} ###
"""Base class for document retrieval for various connections."""

import logging
import os
import sys
import re
from subprocess import check_call, CalledProcessError

ENCODING = 'utf-8'


class DocumentRetrievalFactory:
    """Factory class to create the right sort of retrieval object."""

    @staticmethod
    def _get_matching_connection(datasource, cluster, connections):
        """Get connection matching datasource and cluster."""
        for conn in connections:
            if conn['datasource'] == datasource and conn['cluster'] == cluster:
                return conn
        return None

    @staticmethod
    def create(application, key):
        """Lookup connection and create required type."""
        connection = DocumentRetrievalFactory._get_matching_connection(key.datasource, key.cluster, application.conn_details)
        try:
            platform = connection['platform']
            if platform == 'IBM COS':
                _, client, connection = application.create_cos_connection(connection)
                return DocumentRetrievalCOS(client, connection)
            elif platform == 'NFS':
                _, client, connection = application.create_nfs_connection(connection)
                return DocumentRetrievalNFS(client, connection)
            elif platform == 'Spectrum Scale':
                platform, client, connection = application.create_scale_connection(connection)
                if platform == 'Spectrum Scale Local':
                    return DocumentRetrievalLocalScale(client, connection)
                return DocumentRetrievalScale(client, connection)
            elif platform == 'SMB/CIFS':
                _, client, connection = application.create_smb_connection(connection, key.fileset)
                return DocumentRetrievalSMB(client, connection)
        except (TypeError, KeyError):
            # No connection found
            pass
        return None


class DocumentRetrievalBase():
    """A class to retrieve a document via a Spectrum Discover Connection."""

    def __init__(self, client, connection):
        """Constructor."""
        # Instantiate logger
        loglevels = {'INFO': logging.INFO, 'DEBUG': logging.DEBUG,
                     'ERROR': logging.ERROR, 'WARNING': logging.WARNING}
        log_level = os.environ.get('LOG_LEVEL', 'DEBUG')
        log_format = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        logging.basicConfig(stream=sys.stdout,
                            format=log_format,
                            level=loglevels[log_level])
        self.logger = logging.getLogger(__name__)

        self.client = client
        self.connection = connection

        self.stat_atime = None
        self.stat_mtime = None

        try:
            self.preserve_stat_time = self.connection['additional_info']['preserve_stat_time']
        except (KeyError, TypeError):
            self.preserve_stat_time = False

    def get_document(self, key):
        """
        To be implemented by the child class.

        Receives a document key that uniquely identifies a particular
        object, and is responsible for retrieving that object and
        returning the filepath as a string.

        Returns None if object was not able to be retrieved.
        """
        self.logger.warning("get_document has not been implemented for this class")

    def get_headers(self, key):
        """
        To be implemented by the child class.

        Receives a document bucket and key that uniquely identifies a particular
        cos object, and is responsible for retrieving that objects headers.

        Returns None if object was not able to be retrieved.
        """
        self.logger.warning("get_headers has not been implemented for this class")

    def cleanup_document(self):
        """
        To be implemented by the child class.

        Perform cleanup and/or remove any tmp files as needed.

        Returns None.
        """
        self.logger.warning("cleanup_document has not been implemented for this class")

    def close_connection(self):
        """
        To be implemented by the child class if needed.

        Closes an open connection.

        Returns None.
        """

    def create_file_path(self, prefix, filetype):
        """Return the tmpfile_path with filetype as string."""
        self.logger.debug('filepath prefix: %s, type: %s', prefix, filetype)
        if filetype:
            return prefix + "." + filetype
        return prefix

    def save_stat_times(self, filepath, method=os, nanoseconds=True):
        """
        Save the stat info for the given file.

        If nanoseconds=True, nano-second results will be preserved, else seconds will be preserved.
        By default, use the os module. For remote scale connections, use paramiko sftp module.
        """
        self.logger.debug('Attempting to get stat info for %s', filepath)
        try:
            stat = method.stat(filepath)
            if nanoseconds:
                self.stat_atime = stat.st_atime_ns
                self.stat_mtime = stat.st_mtime_ns
            else:
                self.stat_atime = stat.st_atime
                self.stat_mtime = stat.st_mtime

            self.logger.debug('Successfully retrieved stat info for %s. atime: %s, mtime: %s', filepath, str(self.stat_atime),
                              str(self.stat_mtime))
        except (PermissionError, FileNotFoundError) as error:
            self.logger.error('Failed to retrieve stat info for %s. Error: %s', filepath, str(error))
            self.stat_atime = None
            self.stat_mtime = None

    def restore_stat_times(self, filepath, method=os, nanoseconds=True):
        """Restore the stat info (atime, mtime) for the given file."""
        if self.stat_atime and self.stat_mtime:
            self.logger.debug('Attempting to restore stat info for %s', filepath)
            try:
                if nanoseconds:
                    method.utime(filepath, ns=(self.stat_atime, self.stat_mtime))
                else:
                    method.utime(filepath, (self.stat_atime, self.stat_mtime))

                self.logger.debug('Successfully restored stat info for %s. atime: %s, mtime: %s', filepath, str(self.stat_atime),
                                  str(self.stat_mtime))
            except (PermissionError, FileNotFoundError, OSError) as error:
                self.logger.error('Failed to restore stat info for %s. Error: %s', filepath, str(error))

class DocumentRetrievalCOS(DocumentRetrievalBase):
    """Create a COS document class.

    This will act appropriately upon COS documents that have been downloaded as temp files.
    """

    filepath = None

    def get_document(self, key):
        """Return document filepath."""
        content = None
        self.filepath = None

        if self.client:
            obj = self.client.get_object(Bucket=key.datasource, Key=key.path.decode(ENCODING).split('/', 1)[1])
            content = obj['Body'].read()
            self.filepath = self.create_file_path('/tmp/cosfile_' + str(os.getpid()), key.filetype)
            with open(self.filepath, 'w', encoding=ENCODING) as file:
                file.write(content.decode(ENCODING, 'replace'))
        else:
            self.logger.error('Could not access file %s', key.path.decode(ENCODING))

        return self.filepath

    def get_headers(self, key):
        """Return the COS HTTPHeaders for a key, bucket."""
        http_headers = None

        if self.client:
            vault, doc_name = key.path.decode(ENCODING).split('/', 1)
            headers = self.client.head_object(Bucket=vault, Key=doc_name)
            try:
                http_headers = headers['ResponseMetadata']['HTTPHeaders']
            except KeyError:
                self.logger.error('Could not access header metadata information for file %s: %s', key.path.decode(ENCODING), headers)
        else:
            self.logger.error('Could not access client')
        return http_headers

    def cleanup_document(self):
        """Cleanup files as needed."""
        self.logger.debug("COS: Attempting to delete file: %s", self.filepath)

        try:
            os.remove(self.filepath)
        except FileNotFoundError:
            self.logger.debug('No file: %s to delete.', self.filepath)


class DocumentRetrievalNFS(DocumentRetrievalBase):
    """Create a NFS document class.

    This will act appropriately upon nfs documents.
    No need to download or cleanup since accessed direclty upon the mount point.
    """

    filepath = None

    def get_document(self, key):
        """Return document filepath."""
        self.filepath = None

        if self.connection:
            mount_path_prefix = self.connection['additional_info']['local_mount']
            source_path_prefix = self.connection['mount_point']
            self.filepath = re.sub('^' + source_path_prefix, mount_path_prefix, key.path.decode(ENCODING))
        else:
            self.logger.info('No document match')

        if self.preserve_stat_time:
            self.save_stat_times(self.filepath)

        return self.filepath

    def cleanup_document(self):
        """Cleanup files as needed."""
        if self.preserve_stat_time:
            self.restore_stat_times(self.filepath)

        self.logger.debug("NFS: Not doing any cleanup.")

    def close_connection(self):
        """Unmounts an nfs connection."""
        mount_point = self.connection['additional_info']['local_mount']
        cmd = f'umount {mount_point}'
        try:
            check_call(cmd, shell=True)
        except CalledProcessError as cpe:
            self.logger.warning('Failed to to unmount NFS export %s for connection %s. Error: %s', mount_point,
                                self.connection['name'], str(cpe))


class DocumentRetrievalScale(DocumentRetrievalBase):
    """Create a Spectrum Scale document class.

    This will act appropriately upon Scale documents that have been downloaded as temp files.
    """

    filepath = None
    key = None

    def get_document(self, key):
        """Return document filepath."""
        self.filepath = None
        self.key = key

        if self.preserve_stat_time:
            self.save_stat_times(self.key.path.decode(ENCODING), method=self.client, nanoseconds=False)

        if self.client:
            try:
                self.filepath = self.create_file_path('/tmp/scalefile_' + str(os.getpid()), self.key.filetype)
                self.client.get(self.key.path, self.filepath)
            except (UnicodeDecodeError, FileNotFoundError, OSError) as error:
                if isinstance(error, UnicodeDecodeError):
                    self.logger.error('Could not decode file %s', self.key.path.decode(ENCODING))
                elif isinstance(error, FileNotFoundError):
                    self.logger.error('Could not find file %s', self.key.path.decode(ENCODING))
                elif isinstance(error, OSError):  # Seen when scale nsd disk is down and file is not accessible
                    self.logger.error('Could not transfer file %s', self.key.path.decode(ENCODING))
                # On an error, paramiko will create an empty file that needs to be cleaned up.
                self.cleanup_document()
                self.filepath = None

        else:
            self.logger.info('No document match')

        return self.filepath

    def cleanup_document(self):
        """Cleanup files as needed."""
        self.logger.debug("SCALE: Attempting to delete file: %s", self.filepath)

        if self.preserve_stat_time:
            self.restore_stat_times(self.key.path.decode(ENCODING), method=self.client, nanoseconds=False)

        try:
            os.remove(self.filepath)
        except FileNotFoundError:
            self.logger.debug('No file: %s to delete.', self.filepath)

    def close_connection(self):
        """Close an active sftp connection."""
        self.client.close()


class DocumentRetrievalLocalScale(DocumentRetrievalBase):
    """Create a Local Spectrum Scale document class.

    This will act appropriately upon nfs documents.
    No need to download or cleanup since accessed direclty upon the mount point.
    """

    filepath = None

    def get_document(self, key):
        """Return Document Key."""
        self.filepath = key.path.decode(ENCODING)

        if self.preserve_stat_time:
            self.save_stat_times(self.filepath)

        return self.filepath

    def cleanup_document(self):
        """Cleanup files as needed."""
        if self.preserve_stat_time:
            self.restore_stat_times(self.filepath)

        self.logger.debug("Scale Local: Not doing any cleanup.")


class DocumentRetrievalSMB(DocumentRetrievalBase):
    """Create a SMB document class.

    This will act appropriately upon smb documents.
    No need to download or cleanup since accessed direclty upon the mount point.
    """

    filepath = None

    def get_document(self, key):
        """Return document filepath."""
        self.filepath = None

        if self.connection:
            mount_path_prefix = self.connection['additional_info']['local_mount']
            self.filepath = mount_path_prefix + key.path.decode(ENCODING)
        else:
            self.logger.info('No document match')

        if self.preserve_stat_time:
            self.save_stat_times(self.filepath)

        return self.filepath

    def cleanup_document(self):
        """Cleanup files as needed."""
        if self.preserve_stat_time:
            self.restore_stat_times(self.filepath)

        self.logger.debug("SMB: Not doing any cleanup.")

    def close_connection(self):
        """Unmounts a smb connection."""
        mount_point = self.connection['additional_info']['local_mount']
        cmd = f'umount {mount_point}'
        try:
            check_call(cmd, shell=True)
        except CalledProcessError as cpe:
            self.logger.warning('Failed to to unmount SMB export %s for connection %s. Error: %s', mount_point,
                                self.connection['name'], str(cpe))


class DocumentKey(object):
    """A class to identify a unique document on a Spectrum Discover Connection."""

    def __init__(self, doc):
        """Init."""
        self.fkey = doc['fkey']
        self.datasource = doc['datasource']
        self.cluster = doc['cluster']
        self.path = doc['path'].encode(ENCODING)
        self.filetype = doc['type'] if 'type' in doc.keys() else None  # deepinspect
        self.fileset = doc['fileset'] if 'fileset' in doc.keys() else None  # 2.0.3.1+
        # a unique identifier for the connection this document belongs to.
        self.id = self.datasource + ':' + self.cluster

    def __str__(self):
        """Return string formatted doc info."""
        return "{}/{} -> {} (fkey: {})".format(self.datasource, self.cluster, self.path, self.fkey)
