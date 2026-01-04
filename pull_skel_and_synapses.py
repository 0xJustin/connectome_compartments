import neuprint as neu
import navis.interfaces.neuprint as nav_neu
import pandas as pd
import numpy as np
import json

# 1. AUTHENTICATE
with open('./token.json') as f:
    token_data = json.load(f)
token = token_data["neuprint_token"]
client = neu.Client('neuprint.janelia.org', dataset='male-cns:v0.9', token=token, progress=False)


target_type = "APL"
# 2. FETCH THE NEURON
q = f"""
MATCH (n:Neuron)
WHERE n.type = '{target_type}'
RETURN n.bodyId AS bodyId, n.instance AS instance, n.pre AS num_pre, n.post AS num_post
"""
df = client.fetch_custom(q)
print(f"Found {len(df)} target neurons.")
print(df.head())

# Select the first target found
target_body_id = df.iloc[0]['bodyId']

print("Fetching skeleton and synapse locations...")
target_neuron = nav_neu.fetch_skeletons(
    target_body_id, 
    client=client, 
    heal=True, 
    with_synapses=True
)[0]

from neuprint import NeuronCriteria as NC, SynapseCriteria as SC, fetch_synapses, fetch_neurons, fetch_adjacencies
pre_adjacencies = fetch_adjacencies(
    sources=NC(bodyId=target_body_id),
)[0]
post_adjacencies = fetch_adjacencies(
    targets=NC(bodyId=target_body_id),
)[0]

pre_ids = pre_adjacencies.bodyId.unique()
post_ids = post_adjacencies.bodyId.unique()

from neuprint import NeuronCriteria as NC, SynapseCriteria as SC, fetch_synapses, fetch_neurons
from tqdm import tqdm
from neuprint import Client, fetch_synapse_connections, NeuronCriteria as NC, SynapseCriteria as SC
pre_ids = pre_adjacencies.bodyId.unique()
post_ids = post_adjacencies.bodyId.unique()

import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm

# Settings
BATCH_SIZE = 200
MAX_WORKERS = 20

import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm

# Settings
BATCH_SIZE = 200
MAX_WORKERS = 20

def fetch_batch_target_post(batch_ids):
    """
    Helper function to fetch a single batch. 
    Returns the dataframe or an empty DataFrame on error/empty.
    """
    try:
        return fetch_synapse_connections(
            source_criteria=batch_ids,
            target_criteria=target_body_id,
            synapse_criteria=SC(primary_only=True),
            min_total_weight=5,
            client=client
        )
    except Exception as e:
        print(f"Error fetching batch: {e}")
        return pd.DataFrame() # Return empty on failure to keep the loop alive

batches = [pre_ids[i:i + BATCH_SIZE] for i in range(0, len(pre_ids), BATCH_SIZE)]

# 2. Execute in Parallel
connections_list_target_post = []

print(f"Fetching with {MAX_WORKERS} threads...")
with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
    # Submit all tasks to the pool
    # We map the future back to the batch just in case we need to debug
    future_to_batch = {executor.submit(fetch_batch_target_post, batch): batch for batch in batches}
    
    # Process as they complete (with progress bar)
    for future in tqdm(as_completed(future_to_batch), total=len(batches)):
        result_df = future.result()
        if not result_df.empty:
            connections_list_target_post.append(result_df)

# 3. Concatenate
if connections_list_target_post:
    connections_list_target_post = pd.concat(connections_list_target_post, ignore_index=True)
else:
    connections_list_target_post = pd.DataFrame()

def fetch_batch_target_pre(batch_ids):
    """
    Helper function to fetch a single batch. 
    Returns the dataframe or an empty DataFrame on error/empty.
    """
    try:
        return fetch_synapse_connections(
            source_criteria=target_body_id,
            target_criteria=batch_ids,
            synapse_criteria=SC(primary_only=True),
            min_total_weight=5,
            client=client
        )
    except Exception as e:
        print(f"Error fetching batch: {e}")
        return pd.DataFrame() # Return empty on failure to keep the loop alive

batches = [post_ids[i:i + BATCH_SIZE] for i in range(0, len(post_ids), BATCH_SIZE)]

# 2. Execute in Parallel
connections_list_target_pre = []

print(f"Fetching with {MAX_WORKERS} threads...")
with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
    # Submit all tasks to the pool
    # We map the future back to the batch just in case we need to debug
    future_to_batch = {executor.submit(fetch_batch_target_pre, batch): batch for batch in batches}
    
    # Process as they complete (with progress bar)
    for future in tqdm(as_completed(future_to_batch), total=len(batches)):
        result_df = future.result()
        if not result_df.empty:
            connections_list_target_pre.append(result_df)

# 3. Concatenate
# ---------------------------------------------------------
if connections_list_target_pre:
    connections_list_target_pre = pd.concat(connections_list_target_pre, ignore_index=True)
else:
    connections_list_target_pre = pd.DataFrame()

unique_neurons = list(set(connections_list_target_post['bodyId_post']).union(
    set(connections_list_target_pre['bodyId_pre']))
)
neuron_info = fetch_neurons(NC(bodyId=unique_neurons), returned_columns=['bodyId', 'instance', 'type'])[0][["bodyId", "instance", "type"]]

# merge neuron info with presynaptic connections
presynaptic_connections = connections_list_target_post.merge(
    neuron_info, left_on='bodyId_post', right_on='bodyId', how='left', suffixes=('', '_neuron')
).drop(columns=['bodyId'])

postsynaptic_connections = connections_list_target_pre.merge(
    neuron_info, left_on='bodyId_pre', right_on='bodyId', how='left', suffixes=('', '_neuron')
).drop(columns=['bodyId'])

target_neuron.to_swc(f"{target_type}_{target_body_id}_skeleton_with_synapses.swc")
presynaptic_connections.to_csv(f"{target_type}_{target_body_id}_presynaptic_connections.csv", index=False)
postsynaptic_connections.to_csv(f"{target_type}_{target_body_id}_postsynaptic_connections.csv", index=False)