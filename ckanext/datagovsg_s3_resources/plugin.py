"""plugin.py

DatagovsgS3ResourcesPlugin
Extends plugins.SingletonPlugin
"""

import logging
import datetime
import ckan.plugins as plugins
from routes.mapper import SubMapper
import ckanext.datagovsg_s3_resources.upload as upload


class DatagovsgS3ResourcesPlugin(plugins.SingletonPlugin):
    '''
    DatagovsgS3ResourcesPlugin
    Extends plugins.SingletonPlugin

    1. Connects package and resource download routes
    2. Hooks into before_create, before_update to upload resource to S3
    3. Hooks into after_create, after_update to upload resource zipfile to S3
    '''

    plugins.implements(plugins.IResourceController, inherit=True)
    plugins.implements(plugins.IRoutes, inherit=True)

    ##############################################################
    # IRoutes ####################################################
    ##############################################################

    def before_map(self, map):
        '''Connect our package controller to resource download action'''
        m = SubMapper(
            map,
            controller='ckanext.datagovsg_s3_resources.controllers.package:\
                S3ResourcesPackageController')
        # Connect routes for resource download
        m.connect(
            'resource_download',
            '/dataset/{id}/resource/{resource_id}/download',
            action="resource_download")
        return map


    ##############################################################
    # IResourceController ########################################
    ##############################################################

    def before_create_or_update(self, context, resource):
        '''before_create_or_update - our own function. NOT a CKAN hook.
        Contains shared code performed regardless of whether we are
        creating or updating.
        '''

        # Check if required config options exist
        if not upload.config_exists():
            # Log an error
            logger = logging.getLogger(__name__)
            logger.error("Required S3 config options missing. Please check if required config options exist.")
            raise Exception('Required S3 config options missing')
        else:
            # If resource is an API, don't do anything special
            if resource.get('format') == 'API':
                return
            # Only upload to S3 if not blacklisted
            elif not upload.is_blacklisted(resource):
                upload.upload_resource_to_s3(context, resource)
            else:
                # If blacklisted, the resource file is uploaded to CKAN.
                # 
                # However, in the CKAN source resource_create/resource_update, package_update is 
                # called before the file is uploaded.
                # 
                # This causes a problem as our package after_update attempts to upload
                # the package zipfile and it cannot locate the resource file.
                # 
                # To solve this, we add the field 'resource_create_or_update' into the context object,
                # and look for it in the package after_update.
                # 
                # We remove this field from the context object in resource after_create and after_update.
                # 
                # We don't actually use the value context['resource_create_or_update'], we just check
                # the existence of 'resource_create_or_update' in context.
                context['resource_create_or_update'] = True

                logger = logging.getLogger(__name__)
                logger.info("Resource %s from package %s is blacklisted and not uploaded to S3." % (resource['name'], resource['package_id']))


    def before_create(self, context, resource):
        '''Runs before resource_create. Modifies resource destructively to put in the S3 URL'''
        self.before_create_or_update(context, resource)

    def before_update(self, context, _, resource):
        '''Runs before resource_update. Modifies resource destructively to put in the S3 URL'''
        self.before_create_or_update(context, resource)

