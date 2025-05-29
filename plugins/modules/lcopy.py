
# -*- coding: utf-8 -*-
#!/usr/bin/python


# GNU General Public License v3.0+ (see COPYING or https://www.gnu.org/licenses/gpl-3.0.txt)

from __future__ import absolute_import, division, print_function
__metaclass__ = type

import os
import stat
import shutil
import ansible_runner
from multiprocessing.pool import ThreadPool
from ansible.module_utils.basic import AnsibleModule




DOCUMENTATION = r"""
---
module: copy
version_added: historical
short_description: Copy large files to remote locations in parallel
"""

EXAMPLES = r"""
- name: Copy large file with owner and permissions
  hosts: all
  tasks:
    - name: Use lcopy module
      lcopy:
        src: /tmp/testfile
        dest: /tmp/testfile_copy
        mode: '0644'
        chunk_size: 128
"""        


def split_file(src, chunk_dir, chunk_size):
    """Split file into chunks in chunk_dir, return list of chunk paths."""
    chunk_paths = []
    with open(src, 'rb') as f:
        i = 0
        while True:
            chunk = f.read(chunk_size)
            if not chunk:
                break
            chunk_name = os.path.join(chunk_dir, f"chunk_{i:05d}")
            with open(chunk_name, 'wb') as cf:
                cf.write(chunk)
            chunk_paths.append(chunk_name)
            i += 1
    return chunk_paths


def copy_chunk(chunk_path, dest_chunk_path):
    """Copy a single chunk file to its destination."""
    shutil.copy2(chunk_path, dest_chunk_path)
    return dest_chunk_path


def reassemble_file(chunk_paths, dest_file):
    """Concatenate chunk files into a single output file."""
    with open(dest_file, 'wb') as out:
        for chunk in chunk_paths:
            with open(chunk, 'rb') as c:
                shutil.copyfileobj(c, out)


def do_chunk_copy(src,dest,chunk_dir,chunk_size,workers):
    # Split source file into chunks
    chunk_paths = split_file(src, chunk_dir, chunk_size)

    # Copy chunks in parallel
    dest_chunk_paths = [os.path.join(chunk_dir, f"dest_chunk_{i:05d}") for i in range(len(chunk_paths))]
    with ThreadPool(workers) as pool:
        pool.starmap(copy_chunk, zip(chunk_paths, dest_chunk_paths))

    # Reassemble into final file
    reassemble_file(dest_chunk_paths, dest)

def main():
    module_args = dict(
        src=dict(type='str', required=True),
        dest=dict(type='str', required=True),
        mode=dict(type='str', required=False),
        owner=dict(type='str', required=False),
        group=dict(type='str', required=False),
        backup=dict(type='bool', default=False),
        force=dict(type='bool', default=True),
        chunk_size=dict(type='int', default=64),  # in MB
        file_size_threshold=dict(type='int', default=64),  # in GB
        workers = dict(type='int', default=4), # Number of parallel threads
    )

    result = dict(changed=False, msg='')

    module = AnsibleModule(
        argument_spec=module_args,
        supports_check_mode=True
    )

    src = module.params['src']
    dest = module.params['dest']
    mode = module.params['mode']
    owner = module.params['owner']
    group = module.params['group']
    backup = module.params['backup']
    force = module.params['force']
    chunk_size_mb = module.params['chunk_size']
    chunk_size = chunk_size_mb * 1024 * 1024  # Convert to bytes
    file_size_threshold =  module.params['file_size_threshold']
    file_size_threshold = file_size_threshold * 1024 * 1024 * 1024 # Convert to bytes
    workers = module.params['workers']

    if not os.path.exists(src):
        module.fail_json(msg=f"Source file '{src}' not found", **result)

    if os.path.isdir(dest):
        dest = os.path.join(dest, os.path.basename(src))

    if not force and os.path.exists(dest):
        module.exit_json(changed=False, msg="File exists and force is false", **result)

    if module.check_mode:
        module.exit_json(changed=True, msg="Check mode: file would be copied", **result)

    temp_dir = os.path.dirname(dest)
    chunk_dir = os.path.join(temp_dir, '.copy_chunks_tmp')
    os.makedirs(chunk_dir, exist_ok=True)

    try:
        if os.path.exists(dest) and backup:
            backup_path = dest + ".bak"
            shutil.copy2(dest, backup_path)


        file_size = os.path.getsize(src)
        if file_size > file_size_threshold:
            do_chunk_copy(src,dest,chunk_dir,chunk_size,workers)
            result['msg'] = f"File >{file_size_threshold}GB; used parallel chunked copy ({chunk_size_mb}MB chunks)"
        else:
            r = ansible_runner.run(
                module='copy',
                module_args='src={src} dest={dest} owner={owner} group={group} mode={mode}',
                host_pattern='localhost'
                )
            result['msg'] = "File ≤{file_size_threshold}; use normal copy for this task"

        if mode:
            os.chmod(dest, int(mode, 8))

        if owner or group:
            import pwd, grp
            uid = pwd.getpwnam(owner).pw_uid if owner else -1
            gid = grp.getgrnam(group).gr_gid if group else -1
            os.chown(dest, uid, gid)

        result['changed'] = True
        result['msg'] = f"Copied '{src}' to '{dest}' using parallel chunked copy ({chunk_size_mb} MB)"
    except Exception as e:
        module.fail_json(msg=f"Failed: {e}", **result)
    finally:
        # Cleanup temp chunk files
        for f in os.listdir(chunk_dir):
            os.remove(os.path.join(chunk_dir, f))
        os.rmdir(chunk_dir)

    module.exit_json(**result)

if __name__ == '__main__':
    main()