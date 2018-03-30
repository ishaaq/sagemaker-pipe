import unittest
import sagemaker_pipe as sm_pipe
import shutil, tempfile
import os
import stat

class TestSageMakerPipe(unittest.TestCase):
    def setUp(self):
        # Create a temporary directory
        self.test_dir = tempfile.mkdtemp()

    def tearDown(self):
        # Remove the directory after the test
        shutil.rmtree(self.test_dir)

    def test_fifo_path(self):
        self.assertEqual(sm_pipe.fifo_path('dest', 'channel', 0),
                         'dest/channel_0', 'Unexpected fifo path')

    def test_create_fifo(self):
        fifo_path = sm_pipe.create_fifo(self.test_dir, 'training', 1)
        self.assertTrue(stat.S_ISFIFO(os.stat(fifo_path).st_mode),
                        'Expected a fifo file to have been created')

        fifo_path = sm_pipe.create_fifo(self.test_dir + '/nested',
                                        'training', 1)
        self.assertTrue(stat.S_ISFIFO(os.stat(fifo_path).st_mode),
                        'Expected a fifo file to have been created')

    def test_delete_fifo(self):
        fifo_path = sm_pipe.create_fifo(self.test_dir, 'training', 1)
        self.assertTrue(stat.S_ISFIFO(os.stat(fifo_path).st_mode),
                        'Expected a fifo file to have been created')

        sm_pipe.delete_fifo(self.test_dir, 'training', 1)
        self.assertFalse(os.path.exists(fifo_path),
                         'Expected the file to have been deleted')
