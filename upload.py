import os
import math
import json
import tempfile
import argparse

import boto3
import botocore.utils


class Upload(object):
    def __init__(self, parsed):
        if parsed.resume_from_err:
            fp = open(parsed.path, 'r')
            data = json.load(fp)

            self.verbose = parsed.verbose
            self.err = True
            self.vault_name = data['vault_name']
            self.file_path = data['file_path']
            self.file_size = os.path.getsize(self.file_path)
            self.chunk_size_mb = data['chunk_size_mb']
            self.chunk_size = 1048576 * self.chunk_size_mb
            self.chunks = math.ceil(self.file_size / self.chunk_size)

            self.file = open(self.file_path, 'rb')
            self.cur_chunk = data['cur_chunk']
            self.cur_byte = data['cur_byte']

            glacier = boto3.resource('glacier')
            self.upload_obj = glacier.MultipartUpload(data['account_id'],
                                                      data['vault_name'],
                                                      data['upload_id'])
            self.part_checksums = data['part_checksums']
        else:
            self.err = False
            self.vault_name = parsed.vault
            self.description = parsed.desc
            self.verbose = parsed.verbose

            self.file_path = parsed.path
            self.file_size = os.path.getsize(parsed.path) 
            self.chunk_size_mb = parsed.chunk_size_mb
            self.chunk_size = 1048576 * self.chunk_size_mb
            self.chunks = math.ceil(self.file_size / self.chunk_size)

            self.file = open(self.file_path, 'rb')
            self.cur_chunk = 0
            self.cur_byte = 0
            self.cur_byte_end = 0
            self.cur_part_data = None
            self.upload_obj = None
            self.part_checksums = {}

        if self.verbose:
            print('path: %s, fsize: %s, csize: %s, nchunks: %s' % (
                  self.file_path, self.file_size, self.chunk_size, self.chunks))
    
    def _dump_state(self):
        state = {
            'account_id': self.upload_obj.account_id,
            'vault_name': self.vault_name,
            'upload_id': self.upload_obj.id,
            'cur_chunk': self.cur_chunk,
            'cur_byte': self.cur_byte,
            'file_path': self.file_path,
            'chunk_size_mb': self.chunk_size_mb,
            'part_checksums': self.part_checksums
        }

        with tempfile.NamedTemporaryFile(prefix='dump-', dir=os.getcwd(),
                                         mode='w+', delete=False) as file:
            json.dump(state, file)

    def _get_current_chunk(self):
        self.cur_part_data = self.file.read(self.chunk_size)
        if not self.cur_part_data:
            return 0
        
        bytes_read = len(self.cur_part_data)
        self.cur_byte_end = self.cur_byte + bytes_read - 1
        self.cur_chunk += 1

        return bytes_read

    def request_upload(self):
        if not self.err:
            client = boto3.client('glacier')
            response = client.initiate_multipart_upload(vaultName=self.vault_name,
                                                        archiveDescription=self.description,
                                                        partSize=str(self.chunk_size))
            upload_id = response['uploadId']
            account_id = boto3.client('sts').get_caller_identity()['Account']

            glacier = boto3.resource('glacier')
            self.upload_obj = glacier.MultipartUpload(account_id, self.vault_name, upload_id)
            
            if self.verbose:
                print(f'Successfully created Glacier upload request with id {upload_id}')

    def start_upload(self):
        bytes_to_mb = lambda x: round(x/1048576, 2)
        bytes_read = self._get_current_chunk()
        try:
            while bytes_read != 0:
                byte_range = f'bytes {self.cur_byte}-{self.cur_byte_end}/*'
                response = self.upload_obj.upload_part(range=byte_range, body=self.cur_part_data)
                checksum = response['checksum']
                self.part_checksums[self.cur_chunk] = checksum
                self.cur_byte += bytes_read

                if self.verbose:
                    print('Uploaded chunk %s of %s (%s MB / %s MB) [%s%%]' % (
                          str(self.cur_chunk),
                          str(self.chunks),
                          str(bytes_to_mb(self.cur_byte_end)),
                          str(bytes_to_mb(self.file_size)),
                          str(round((self.cur_byte_end / self.file_size) * 100, 2))
                    ))
                bytes_read = self._get_current_chunk()

        except Exception as e:
            self._dump_state()
            self.file.close()
            raise e

        self.file.close()

    def finish_upload(self):
        f = open(self.file_path, 'rb')
        file_checksum = botocore.utils.calculate_tree_hash(f)
        f.close()

        response = self.upload_obj.complete(archiveSize=str(self.file_size),
                                            checksum=file_checksum)
        print('Upload successful!')

def parse_args():
    def validate_chunk_size(size):
        if size < 1 or size > 4096:
            raise ValueError('Illegal chunk size: expected value between 1 MiB and 4 GiB' \
                             ', received %s.' % size)
        if size == 0 or (size & (size - 1)):
            raise ValueError('Illegal chunk size: size must be a power of 2.')
        return size

    parser = argparse.ArgumentParser(description='CLI for uploading files to AWS S3 Glacier')
    parser.add_argument('vault', nargs=1, type=str,
                        help='The name of the Glacier vault to upload to')
    parser.add_argument('path', nargs=1, type=str,
                        help='The path to the file to upload')
    parser.add_argument('-v', '--verbose', default=False, action='store_true',
                        help='Show more details while uploading')
    parser.add_argument('-d', '--description', nargs=1, type=str, metavar='DESC', dest='desc',
                        help='The description of this file (defaults to the file\'s name)')
    parser.add_argument('-s', '--chunk-size', nargs=1, type=int, dest='chunk_size_mb', 
                        default=128, metavar='SIZE',
                        help='The size of each upload part, in megabytes. Must be a power of 2.' \
                             ' Defaults to 128 if no value is specified.')
    parser.add_argument('--resume-from-err', default=False, action='store_true',
                        dest='resume_from_err',
                        help='If the upload exited unexpectedly, set this flag and specify the ' \
                             'dumpfile as the file.')
    
    args = parser.parse_args()
    args.vault = args.vault[0]
    args.path = os.path.abspath(args.path[0])
    if not args.desc:
        args.desc = os.path.basename(args.path)
    if type(args.chunk_size_mb) is list:
        args.chunk_size_mb = validate_chunk_size(args.chunk_size_mb[0])

    return args

    
if __name__ == '__main__':
    args = parse_args()
    upload = Upload(args)
    upload.request_upload()
    upload.start_upload()
    upload.finish_upload()
