""" Where the ray commands live """

from smart_open import open 
import os
import yaml
import boto3
import subprocess
import json

import s5cmd_part_generator as spg
import file_map_builder as fmb
import ray

VERBOSE = True

# Provide backward compatibility for configs that use
# single-node field names like `output_dir` instead of
# the distributed `local_output` and omit the remote
# directory settings.
def canonicalize_config(config_dict):
    if 'local_output' not in config_dict and 'output_dir' in config_dict:
        config_dict['local_output'] = config_dict['output_dir']
    if 'remote_input' not in config_dict:
        config_dict['remote_input'] = config_dict.get('local_input')
    if 'remote_working_dir' not in config_dict:
        config_dict['remote_working_dir'] = config_dict.get('working_dir')
    if 'remote_output' not in config_dict:
        config_dict['remote_output'] = config_dict.get('local_output')
    return config_dict

# ===============================================================
# =                     MISC UTILITIES                          =
# ===============================================================
def download_jsonl_parts(config_dict, file_map_json, part_id, num_parts):
    # Uses fancy s5cmd commands to download 1/num_parts'th of the dataset to disk
    config_file = os.path.join(os.path.dirname(config_dict['working_dir']), 'config.yaml') # Replaced get_config_name
    # Download files if they don't exist
    chunk_stems = spg.get_path_chunk_stems(file_map_json, part_id, num_parts)
    local_inputs = [os.path.join(config_dict['local_input'], stem) for stem in chunk_stems]


    print("LOCAL INPUTS", local_inputs)
    if not all(os.path.exists(_) for _ in local_inputs):
        s5cmd_command = spg.clickfree_get_s5cmd_generator(config_file, part_id, num_parts)
        s5cmd_args = "s5cmd run %s" % s5cmd_command
        sb_run(s5cmd_args)


def sb_run(argstring):
    if VERBOSE:
        print("(Running) %s" % argstring)
    process = subprocess.Popen(argstring.split(' '),
							   text=True, 
							   stdout=subprocess.PIPE,
							   stderr=subprocess.PIPE,
							   bufsize=1,
							   universal_newlines=True)
    for line in process.stdout:
        print(line, end='')
    if process.wait() != 0:
        raise subprocess.CalledProcessError(process.returncode, ['COMMAND'])


# ===============================================================
# =                    SETUP AND TEARDOWN STUFF                 =
# ===============================================================

def shared_file_setup(config_dict):
        # Normalize keys from various example configs
        config_dict = canonicalize_config(config_dict)

        # Make local directories
        for k in ['local_input', 'working_dir', 'local_output']:
                os.makedirs(config_dict[k], exist_ok=True)

        # And download the config file
        config_file = os.path.join(os.path.dirname(config_dict['working_dir']), 'config.yaml') # Replaced get_config_name

        if not os.path.exists(config_file):
            with open(config_file, 'w') as f:
                yaml.dump(config_dict, f)

def download_file_map(config_dict):
    # Downloads the filemap.json.gz onto disk
    local_file_map = os.path.join(config_dict["working_dir"], "filemap.json.gz")
    remote_file_map = os.path.join(config_dict['remote_working_dir'], 'filemap.json.gz')

    if not os.path.exists(local_file_map):
        file_map_data = open(remote_file_map, 'rb').read()
        with open(local_file_map, 'wb') as f:
            f.write(file_map_data)
    else:
        file_map_data = open(local_file_map, 'rb').read()

    return json.loads(file_map_data)


def download_intermed_files(config_dict, subname):
    # Downloads some subcomponent of the working directory from S3
    s5cmd_args = "s5cmd cp -sp %s %s" % (os.path.join(config_dict['remote_working_dir'], subname, '*'),
										 os.path.join(config_dict['working_dir'], subname))
    sb_run(s5cmd_args)




def upload_and_clean(config_dict, subname):
    # Uploads some subcomponent of the local directory to s3

    # Upload first
    upload_args = "s5cmd cp -sp %s %s" % (os.path.join(config_dict['working_dir'], subname, '*'),
									  os.path.join(config_dict['remote_working_dir'], subname, ''))
    sb_run(upload_args)


    # And then clean up
    rm_call = "rm -rf %s" % (os.path.join(config_dict['working_dir'], subname))
    sb_run(rm_call)





# ===============================================================
# =                     RUST ORCHESTRATORS                      =
# ===============================================================


def is_s3_path(path_string):
    return isinstance(path_string, str) and path_string.startswith('s3://')


def call_build_file_map(config_dict, local_sys=True):
    """ Makes the rust call to build the filemap.
        This can be done on the head node.

        Will write the local filemap and the REMOTE filemap
    """

    if not local_sys:
        shared_file_setup(config_dict)

    is_local_scenario = local_sys and not is_s3_path(config_dict.get('remote_input', ''))

    if is_local_scenario:
        local_input_path = config_dict.get('local_input')
        if not local_input_path or not os.path.isdir(local_input_path):
            raise ValueError(f"local_input path {local_input_path} is invalid or not a directory for local run.")

        files_to_map = []
        for fname in os.listdir(local_input_path):
            if fname.endswith('.jsonl'): # Assuming .jsonl, adjust if needed
                files_to_map.append(os.path.join(local_input_path, fname))

        file_map_contents = {
            'local_input': local_input_path,
            'remote_input': config_dict.get('remote_input'), # Keep original remote_input if any, or could be local_input_path
            'indices': {os.path.relpath(p, local_input_path): i for i, p in enumerate(files_to_map)}
        }

        file_map_path = os.path.join(config_dict['working_dir'], 'filemap.json.gz')
        with open(file_map_path, 'wb') as f:
            f.write(json.dumps(file_map_contents).encode('utf-8'))
    else:
        fmb.clickfree_build_file_map(config_dict)

    if is_s3_path(config_dict.get('remote_working_dir', '')):
        local_file_map_data = open(os.path.join(config_dict['working_dir'], 'filemap.json.gz'), 'rb').read()
        remote_file_map = os.path.join(config_dict['remote_working_dir'], 'filemap.json.gz')
        with open(remote_file_map, 'wb') as f:
            f.write(local_file_map_data)


def call_hash_only(config_dict, part_id=0, num_parts=1):
    """ Makes the rust call to do the hashing.
        Uploads these to S3 and then cleans up the local dir when done
    """

    # Setup stuff
    shared_file_setup(config_dict)
    config_file = os.path.join(os.path.dirname(config_dict['working_dir']), 'config.yaml') # Replaced get_config_name
    file_map_json = download_file_map(config_dict)

    # Download the jsonl data
    download_jsonl_parts(config_dict, file_map_json, part_id, num_parts)

    # And then run the rust thing
    hash_args = ("cargo run --release -- hash-only --config %s --path-chunk %s --num-path-chunks %s" % (config_file, part_id, num_parts))
    sb_run(hash_args)

    # Finally, upload results and clean up
    upload_and_clean(config_dict, "sig_storage")


@ray.remote
def ray_hash_only(config_dict, part_id=0, num_parts=1):
    call_hash_only(config_dict, part_id=part_id, num_parts=num_parts)


def call_gather_edges(config_dict):
    """ Makes the rust call to go from hashes to edges.
        Uploads the edge data to s3 and cleans up local dir when done
    """

    # setup
    shared_file_setup(config_dict)
    config_file = os.path.join(os.path.dirname(config_dict['working_dir']), 'config.yaml') # Replaced get_config_name
    file_map_json = download_file_map(config_dict)

    # Download all signatures
    download_intermed_files(config_dict, "sig_storage")


    # And then do the edge gathering
    edge_gather_args = "cargo run --release -- gather-edges --config %s" % config_file
    sb_run(edge_gather_args)
		

    # Finally upload and clean up
    upload_and_clean(config_dict, "edges")


@ray.remote
def ray_gather_edges(config_dict):
    call_gather_edges(config_dict)


def call_build_uf(config_dict, num_parts=1):
    """ Makes the rust call to build the UF structure
        Uploads and cleans the cc and lines-to-kill-data when done
    """

    # Setup
    shared_file_setup(config_dict)
    config_file = os.path.join(os.path.dirname(config_dict['working_dir']), 'config.yaml') # Replaced get_config_name
    file_map_json = download_file_map(config_dict)

    # Download all edges
    download_intermed_files(config_dict, "edges")

    # And then do UF stuff
    uf_args = "cargo run --release -- build-uf --config %s --num-path-chunks %s" % (config_file, num_parts)
    sb_run(uf_args)


    # Finally upload and clean up
    upload_and_clean(config_dict, "ccs")
    upload_and_clean(config_dict, "kill")


@ray.remote
def ray_build_uf(config_dict, num_parts=1):
    call_build_uf(config_dict, num_parts=num_parts)


def call_uf_size_prune(config_dict, path_chunk_id=0, num_path_chunks=1):
    """ Makes the rust call to actually scrub the duplicates from the data pool
        Uploads and cleans everything when done
    """

    shared_file_setup(config_dict)
    config_file = os.path.join(os.path.dirname(config_dict['working_dir']), 'config.yaml') # Replaced get_config_name
    file_map_json = download_file_map(config_dict)


    download_jsonl_parts(config_dict, file_map_json, path_chunk_id, num_path_chunks)
    remote_kill_file = os.path.join(config_dict['remote_working_dir'], 'kill', 'chunk_%08d.kill.bin' % path_chunk_id)
    local_kill_file = os.path.join(config_dict['working_dir'], 'kill', 'chunk_%08d.kill.bin' % path_chunk_id)
    sb_run("s5cmd cp -sp %s %s" % (remote_kill_file, local_kill_file))


    prune_args = "cargo run --release -- uf-size-prune --config %s --path-chunk %s --num-path-chunks %s" % (config_file, path_chunk_id, num_path_chunks)
    sb_run(prune_args)

    # And then upload and clean
    upload_args = "s5cmd cp -sp %s %s" % (os.path.join(config_dict['local_output'], ''),
									      os.path.join(config_dict['remote_output'], ''))
    sb_run(upload_args)


    for clean_dir in [config_dict['local_output'], config_dict['local_input']]:
        rm_call = "rm -rf %s" % clean_dir
        sb_run(rm_call)


@ray.remote
def ray_uf_size_prune(config_dict, path_chunk_id=0, num_path_chunks=1):
        call_uf_size_prune(
            config_dict,
            path_chunk_id=path_chunk_id,
            num_path_chunks=num_path_chunks,
        )



# ================================================
# =                MAIN BLOCK                    =
# ================================================



def main(remote_config):

        # Init ray and do setup
        ray.init(address="auto")
        config_dict = yaml.safe_load(open(remote_config, 'rb').read())
        config_dict = canonicalize_config(config_dict)
        shared_file_setup(config_dict)
        call_build_file_map(config_dict)

        # Do all steps
        num_path_chunks = config_dict['num_path_chunks']
        hash_futures = [ray_hash_only.remote(config_dict, part_id=i, num_parts=num_path_chunks)
					for i in range(num_path_chunks)]
        ray.get(hash_futures)

        ray.get(ray_gather_edges.remote(config_dict))

        ray.get(ray_build_uf.remote(ray_build_uf, num_path_chunks))

        prune_futures = [ray_uf_size_prune.remote(config_dict, path_chunk_id=i, num_path_chunks=num_path_chunks)
					 for i in range(num_path_chunks)]
        ray.get(prune_futures)

