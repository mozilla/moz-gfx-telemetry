#!/usr/bin/env python
# coding: utf-8

# In[ ]:


import ujson as json
import matplotlib.pyplot as plt
import pandas as pd
import numpy as np
import plotly.plotly as py
import operator
import json, time, sys
from __future__ import division
from collections import namedtuple
from moztelemetry import get_pings, get_pings_properties, get_one_ping_per_client
def fmt_date(d):
    return d.strftime("%Y%m%d")
def repartition(pipeline):
    return pipeline.repartition(MaxPartitions).cache()

get_ipython().run_line_magic('pylab', 'inline')

## !!! EDIT THESE BEFORE RUNNING !!!
##
## These represent the number of uniform random samples of raw
## sessions we'll take, from each channel, as a fraction.
##
## When testing on small clusters, it is important to keep the
## fraction low. Using a cluster of size 4, without about 100,000
## pings, the job below took me about 10 minutes. The fraction
## was f=0.001. For iterative development, it is best to use even
## smaller fractions.
##
## As of Nov 2015, 1% (fraction=0.01) of sessions results in
## about 1,000,000 samples for Beta, and about 1/5th that for
## Release. After FHR is removed, the Release population will
## become much, much larger.
BetaFraction = 0.1
ReleaseFraction = 0.03


# In[ ]:


# Disable logging, since this eats a ton of diskspace.
def quiet_logs(sc):
  logger = sc._jvm.org.apache.log4j
  logger.LogManager.getLogger("org").setLevel(logger.Level.ERROR)
  logger.LogManager.getLogger("akka").setLevel(logger.Level.ERROR)
quiet_logs(sc)


# In[ ]:


# Constants.
DaysPerWeek = 7
GfxAdaptersKey = 'environment/system/gfx/adapters'
SystemOsNameKey = 'environment/system/os/name'
AdapterVendorKey = 'vendorID'
AdapterDeviceKey = 'deviceID'
AdapterDriverKey = 'driverVersion'

MajorVendors = {
    u'0x8086': 'Intel',
    u'0x10de': 'NVIDIA',
    u'0x1002': 'AMD',
}


# In[ ]:


def get_pings_for_channel(channel, fraction):
    end_date = datetime.datetime.now()
    start_date = end_date - datetime.timedelta(DaysPerWeek * 3)
    date_range = (fmt_date(start_date), fmt_date(end_date))
    
    args = {
        'app': 'Firefox',
        'schema': 'v4',
        'submission_date': date_range,
        'channel': channel,
        'fraction': fraction,
    }
    
    pings = get_pings(sc, **args)
    pings = get_pings_properties(pings, [
        'clientId',
        GfxAdaptersKey,
        SystemOsNameKey,
    ])
    pings = get_one_ping_per_client(pings)
    
    # Only care about Windows for now.
    pings = pings.filter(lambda p: p.get(SystemOsNameKey, None) == 'Windows_NT')
    pings = pings.filter(has_valid_adapter)
    return pings
    
def has_valid_adapter(p):
    try:
        adapter = p[GfxAdaptersKey][0]
    except:
        return False
    if adapter.get(AdapterVendorKey, 'unknown') not in MajorVendors:
        return False
    if not adapter.get(AdapterDriverKey, None):
        return False
    if not adapter.get(AdapterDeviceKey, None):
        return False
    return True


# In[ ]:


beta_pings = get_pings_for_channel('beta', BetaFraction)
release_pings = get_pings_for_channel('release', ReleaseFraction)

beta_pings = beta_pings.cache()
release_pings = release_pings.cache()

print('Found {0} beta pings.'.format(beta_pings.count()))
print('Found {0} release pings.'.format(release_pings.count()))


# In[ ]:


# Given an RDD, return a map:
#
#   key => Population%
#
# Where the key transform function is given by keyFunc. The
# Population% is the percentage (out of 100) that this key
# occurred in the RDD.
def extract_info_map(pings, keyFunc):
    # countByKey() requires a (key,) form.
    countFunc = lambda p: (keyFunc(p),)
    info_map = pings.map(countFunc).countByKey()
    return compute_map_percentages(pings, info_map)

# Return a function such that given a Telemetry ping, a tuple
# is returned containing each of the named properties on the
# adapter in that ping. For example,
#
#   adapter_subkeys_keyfunc('vendorID', 'deviceID')
#
# Will return a function that does the following transform:
#
#   ping => (vendor, device)
#
def adapter_subkeys_keyfunc(*subkeys):
    def to_key(p):
        adapter = p[GfxAdaptersKey][0]
        return tuple([adapter[key] for key in subkeys])
    return to_key

def compute_map_percentages(pings, info_map):
    output = {}
    total = float(pings.count())
    for key in info_map:
        output[key] = (float(info_map[key]) / total) * 100.0
    return output


# In[ ]:


Diff = namedtuple('Diff', ['missing', 'release', 'beta'])

# Given a list of properties on Adapter that will form a key,
# return a list of tuples, in the format:
#   [(key, population%), ...]
# 
# Where each tuple is a key found in the Release population but not
# Beta, sorted by the rate at which it appears in Release from highest
# to lowest.
def compute_differences(*subkeys):
    keyfunc = adapter_subkeys_keyfunc(*subkeys)
    beta_map = extract_info_map(beta_pings, keyfunc)
    release_map = extract_info_map(release_pings, keyfunc)
    
    out_map = {}
    for key in release_map:
        a_val = release_map[key]
        b_val = beta_map.get(key, 0.0)
        if b_val >= a_val:
            continue
        out_map[key] = (b_val - a_val, b_val, a_val)
    
    missing_sorted = sorted(
        out_map.items(),
        key=lambda obj: obj[1][0])
    
    return Diff(missing_sorted, release_map, beta_map)


# In[ ]:


drivers = compute_differences(AdapterVendorKey, AdapterDriverKey)
devices = compute_differences(AdapterVendorKey, AdapterDeviceKey)
device_and_drivers = compute_differences(AdapterVendorKey, AdapterDeviceKey, AdapterDriverKey)


# In[ ]:


print('-----------------------')
print('Unique drivers: {0} in release, {1} in beta.'.format(
    len(drivers.release),
    len(drivers.beta)))
for ((vendor, driver), (diff, b, a)) in drivers.missing:
    print('  {0} {1} ({2:.3f}%, {3:.3f}% beta, {4:.3f}% release)'.format(MajorVendors[vendor], driver, diff, b, a))
print('-----------------------')


# In[ ]:


print('-----------------------')
print('Unique devices: {0} in release, {1} in beta.'.format(
    len(devices.release),
    len(devices.beta)))
for ((vendor, device), (diff, b, a)) in devices.missing:
    print('  {0} {1} ({2:.3f}%, {3:.3f}% beta, {4:.3f}% release)'.format(MajorVendors[vendor], device, diff, b, a))
print('-----------------------')


# In[ ]:


print('-----------------------')
print('Unique device+driver pairs: {0} in release, {1} in beta.'.format(
    len(device_and_drivers.release),
    len(device_and_drivers.beta)))
for ((vendor, device, driver), (diff, b, a)) in device_and_drivers.missing:
    print('  {0} {1}/{5} ({2:.3f}%, {3:.3f}% beta, {4:.3f}% release)'.format(MajorVendors[vendor], device, diff, b, a, driver))
print('-----------------------')


# In[ ]:




