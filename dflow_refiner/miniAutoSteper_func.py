import os
import shutil
import subprocess
import time
from pathlib import Path
from typing import List, Union, Optional

import pandas as pd
import numpy as np

from ase.atoms import Atom, Atoms
from ase.io import read, write
# ===============================================================================================================
# For miniAutoSteper!!!
# ===============================================================================================================

def de_redundancy(old_df: pd.DataFrame, interval: float, val_clm_name: str):
    new_df = old_df.copy()
    value_arr = new_df[val_clm_name]
    value_arr = (value_arr / interval).round(0)
    clean_idxes = np.unique(ar=value_arr, return_index=True)[1]
    new_df = new_df.loc[clean_idxes]
    new_df.index = range(len(clean_idxes))
    return new_df

def to_36_base(num):
  return ((num == 0) and "0") or (to_36_base(num // 36).lstrip("0") + "0123456789abcdefghijklmnopqrstuvwxyz"[num % 36])


def name2seq(name: str, cage_size: int):
    seq = str()
    addon_set = set()
    bin_str = format(int(name, 36), 'b')
    bin_str = '0' * (cage_size - len(bin_str)) + bin_str
    bin_list = []
    for idx, a_position in enumerate(bin_str):
        if a_position == '1':
            seq += str(idx) + ' '
            addon_set.add(idx)
            bin_list.append(1)
        else:
            bin_list.append(0)
    return seq, addon_set, np.array(bin_list)


def seq2name(seq: str, cage_size: int, cage_max_add_36_size: int):
    addon_list = []
    bin_name = ['0'] * cage_size
    bin_arr = np.zeros(cage_size)
    for i in seq.split():
        int_i = int(i)
        bin_name[int_i] = '1'
        addon_list.append(int_i)
        bin_arr[int_i] = 1
    bin_name_str = ''.join(bin_name)
    base_36_name = to_36_base(int(bin_name_str, 2))
    for i in range(cage_max_add_36_size - len(base_36_name)):
        base_36_name = '0' + base_36_name
    addon_set = set(addon_list)
    return base_36_name, addon_set, bin_arr


def build_unit(old_cage, old_coords, addon_set, dok_matrix, cage_centre, name):
    new_cage = old_cage.copy()
    for i in addon_set:
        ADJ_coords = []
        dok_adj = dok_matrix[i]
        adj_array = dok_adj.tocoo().col
        if len(adj_array) > 3:
            dist = []
            for ii in adj_array:
                dist.append(np.linalg.norm(old_coords[i] - old_coords[ii]))
            map_dict = dict(zip(dist, adj_array))
            sorted_dist = sorted(map_dict.keys())
            for ii in sorted_dist[0:3]:
                ADJ_coords.append(old_coords[map_dict[ii]])
        else:
            for ii in adj_array:
                ADJ_coords.append(old_coords[ii])
        norm_vec_unnormalized = np.cross(ADJ_coords[0] - ADJ_coords[1], ADJ_coords[0] - ADJ_coords[2])
        normal_vec = norm_vec_unnormalized / np.linalg.norm(norm_vec_unnormalized)

        if (old_coords[i] - cage_centre) @ normal_vec > 0:
            addon_coord_0 = old_coords[i] + normal_vec * 1.81
        else:
            addon_coord_0 = old_coords[i] - normal_vec * 1.81

        new_atom_0 = Atom(symbol='Cl', position=addon_coord_0)
        new_cage.append(new_atom_0)
    write(filename=f'{name}.xyz', format='xyz', images=new_cage)

