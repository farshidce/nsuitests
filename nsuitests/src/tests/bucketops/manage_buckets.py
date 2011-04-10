import unittest
import uuid
from common.TestInput import TestInput
from common.logger import Logger
from membase.api.rest_client import RestConnection
from common.ui.navigator import Navigator, Verifier
from membase.helper.bucket_helper import BucketOperationHelper


class BucketTests(unittest.TestCase):

    @classmethod
    def setup_class(klass):
        Logger.start_logger('BucketTests')

    @classmethod
    def tearDown_class(klass):
        """This method is run once for each class _after_ all tests are run"""
        Logger.stop_logger()

    def setUp(self):
        self.navigator = Navigator(TestInput.master())
        BucketOperationHelper.delete_all_buckets_or_assert([TestInput.master()],self)

    def tearDown(self):
        BucketOperationHelper.delete_all_buckets_or_assert([TestInput.master()],self)
        self.navigator.close()

    def test_bucket_create(self):
        rest = RestConnection(ip=TestInput.master(),
                              username=TestInput.get_username(),
                              password=TestInput.get_password())
        #create two buckets
        bucket1 = uuid.uuid4()
        bucket2 = uuid.uuid4()
        rest.create_bucket(bucket=bucket1,replicaNumber=1,ramQuotaMB=128,proxyPort=11220)
        rest.create_bucket(bucket=bucket2,replicaNumber=1,ramQuotaMB=128,proxyPort=11221)
        self.navigator.login(username=TestInput.get_username(),
                             password=TestInput.get_password())
        buckets = rest.get_buckets()
        expected_names = [bucket.name for bucket in buckets]
        assert Verifier(self.navigator).verify_sect_buckets(expected_names)

    def test_bucket_delete(self):
        rest = RestConnection(ip=TestInput.master(),
                              username=TestInput.get_username(),
                              password=TestInput.get_password())
        #create two buckets
        bucket1 = uuid.uuid4()
        bucket2 = uuid.uuid4()
        rest.create_bucket(bucket=bucket1,replicaNumber=1,ramQuotaMB=128,proxyPort=11220)
        rest.create_bucket(bucket=bucket2,replicaNumber=1,ramQuotaMB=128,proxyPort=11221)
        self.navigator.login(username=TestInput.get_username(),
                             password=TestInput.get_password())
        buckets = rest.get_buckets()
        expected_names = [bucket.name for bucket in buckets]
        assert Verifier(self.navigator).verify_sect_buckets(expected_names)
        rest.delete_bucket(bucket=bucket1)
        expected_names = [bucket.name for bucket in buckets]
        assert Verifier(self.navigator).verify_sect_buckets(expected_names)

#    @raises(KeyError)
#    def test_raise_exc_with_decorator(self):
#        Foobar().bye()

