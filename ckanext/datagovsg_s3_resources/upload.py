'''
upload.py

Contains functions that upload the resources/zipfiles to S3.

Also contains the MetadataYAMLDumper class to generate the metadata for zipfiles.
'''
import cgi
import os
import mimetypes
import logging
import datetime

from slugify import slugify
from pylons import config
import requests
import boto3

import ckan.lib.base as base

import ckan.lib.uploader as uploader
import ckan.logic as logic
from ckan.common import _


abort = base.abort
get_action = logic.get_action


class BaseS3Uploader(object):
    def __init__(self):
        self.aws_access_key_id = config.get(
            'ckan.datagovsg_s3_resources.s3_aws_access_key_id')
        self.aws_secret_access_key = config.get(
            'ckan.datagovsg_s3_resources.s3_aws_secret_access_key')
        self.aws_region_name = config.get(
            'ckan.datagovsg_s3_resources.s3_aws_region_name')
        self.bucket_name = config.get(
            'ckan.datagovsg_s3_resources.s3_bucket_name')
        self.s3_url = config.get('ckan.datagovsg_s3_resources.s3_url_prefix')

        if not self.s3_url.endswith('/'):
            self.s3_url += '/'

        if self.aws_region_name:
            s3 = boto3.resource('s3',
                                aws_access_key_id=self.aws_access_key_id,
                                aws_secret_access_key=self.aws_secret_access_key,
                                region_name=self.aws_region_name)
        else:
            s3 = boto3.resource('s3',
                                aws_access_key_id=self.aws_access_key_id,
                                aws_secret_access_key=self.aws_secret_access_key)

        # init logger
        self.logger = logging.getLogger(__name__)

        self.bucket = s3.Bucket(self.bucket_name)


class S3ResourceUploader(BaseS3Uploader):

    def __init__(self):
        super(S3ResourceUploader, self).__init__()

    def upload_resource(self, context, resource):
        """
        upload_resource_to_s3

        Uploads resource to S3 and modifies the following resource fields:
        - 'upload'
        - 'url_type'
        - 'url'
        """

        self.logger.info("Starting upload_resource_to_s3 for resource {}"
                         .format(resource.get('name','')))

        # Get content type and extension
        content_type, _ = mimetypes.guess_type(
            resource.get('url', ''))

        extension = mimetypes.guess_extension(content_type)

        # Upload to S3
        pkg = get_action('package_show')(context,
                                                 {'id': resource['package_id']})

        # should match the assignment in the ResourceUpload class
        timestamp = datetime.datetime.utcnow()

        # get resource name, if not known replace with random string
        resource_name = resource.get('name', '')
        s3_filepath = ('resources'
                       + '/'
                       + pkg.get('name')
                       + '/'
                       + slugify(resource_name, to_lower=True)
                       + '-'
                       + timestamp.strftime("%Y-%m-%dT%H-%M-%SZ")
                       + extension)

        # If file is currently being uploaded, the file is in resource['upload']

        if isinstance(resource.get('upload', None), cgi.FieldStorage):
            self.logger.info("File is being uploaded")
            resource['upload'].file.seek(0)
            body = resource['upload'].file

        # If resource.get('url_type') == 'upload' then the resource is in
        # CKAN file system

        elif resource.get('url_type') == 'upload':
            self.logger.info("File is on CKAN file store")
            upload = uploader.ResourceUpload(resource)
            filepath = upload.get_path(resource['id'])
            try:
                body = open(filepath, 'r')
            except OSError:
                abort(404, _('Resource data not found'))
        else:
            self.logger.info("File is downloadable from URL")
            try:
                # Start session to download files

                session = requests.Session()
                self.logger.info("Attempting to obtain resource {} from url {}"
                                 .format(resource.get('name', ''),
                                         resource.get('url', '')))
                response = session.get(
                    resource.get('url', ''), timeout=30)

                # If the response status code is not 200 (i.e. success),
                # raise Exception

                if response.status_code != 200:
                    self.logger.error("Error obtaining resource from the "
                                      "given URL. Response status code is {}"
                                      .format(response.status_code))
                    raise Exception("Error obtaining resource from the "
                                    "given URL. Response status code is {}"
                                    .format(response.status_code))
                body = response.content
                self.logger.info("Successfully obtained resource %s from url {}"
                                 .format(resource.get('name', ''),
                                         resource.get('url', '')))

            except requests.exceptions.RequestException:
                abort(404, _('Resource data not found'))

        try:
            self.logger.info(
                "Uploading resource {} to S3".format(resource.get('name', '')))
            self.bucket.Object(s3_filepath).delete()
            obj = self.bucket.put_object(Key=s3_filepath,
                                    Body=body,
                                    ContentType=content_type)
            obj.Acl().put(ACL='public-read')
            self.logger.info("Successfully uploaded resource {} to S3".
                             format(resource.get('name','')))

        except Exception as exception:
            # Log the error and reraise the exception

            self.logger.error("Error uploading "
                              "resource {} from package {} to S3"
                              .format(resource['name'], resource['package_id']))
            self.logger.error(exception)
            if resource.get('url_type') == 'upload':
                body.close()
            raise exception

        if resource.get('url_type') == 'upload':
            body.close()

        # Modify fields in resource
        resource['upload'] = ''
        resource['url_type'] = 's3'
        resource['url'] = self.s3_url + config.get(
            'ckan.datagovsg_s3_resources.s3_bucket_name') + '/' + s3_filepath
        self.update_timestamp(resource, timestamp)

    @staticmethod
    def resources_all_api(resources):
        for resource in resources:
            if resource.get('format', '') != 'API':
                return False
        return True

    @staticmethod
    def is_blacklisted(resource):
        """is_blacklisted - Check if the resource type is blacklisted"""

        blacklist = config.get(
            'ckan.datagovsg_s3_resources.upload_filetype_blacklist', '').split()
        blacklist = [t.lower() for t in blacklist]
        resource_format = resource.get('format', '').lower()

        # If resource is being created, format will still be empty.
        # Use file extension instead

        if resource_format == '':
            _, file_ext = os.path.splitext(resource.get('url'))
            resource_format = file_ext[1:].lower()
        return resource_format in blacklist

    @staticmethod
    def update_timestamp(resource, timestamp):
        """use the last modified time if it exists, otherwise use the created
        time. destructively modifies resource """

        if resource.get('last_modified') is None and resource.get(
                'created') is None:
            resource['created'] = timestamp
        else:
            resource['last_modified'] = timestamp
