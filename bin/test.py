#!/usr/bin/python

ua=' GomXXXX'
ua = ua.strip()
if len(ua) < 40 and 'gom' == ua[0:3].lower() :
  print 'ok'
else :
  print 'no'


import os
print os.path.dirname(os.path.realpath(__file__))
print os.path.dirname(os.path.dirname(os.path.realpath(__file__)))

#from __future__ import print_function

import GeoIP

#gi = GeoIP.new(GeoIP.GEOIP_STANDARD)
#gi = GeoIP.new(GeoIP.GEOIP_MMAP_CACHE)
gi = GeoIP.new(GeoIP.GEOIP_MEMORY_CACHE)
#gi = GeoIP.open("/usr/local/share/GeoIP/GeoIP.dat",GeoIP.GEOIP_STANDARD)

print(gi.country_code_by_name("yahoo.com"))
print(gi.last_netmask())
print(gi.country_name_by_name("www.bundestag.de"))
print(gi.country_code_by_addr("24.24.24.24"))
print(gi.country_name_by_addr("24.24.24.24"))
print(gi.range_by_ip("68.180.206.184"))
print(gi.country_code_by_addr("183.110.11.92"))


import sys

print 'Number of arguments:', len(sys.argv), 'arguments.'
print 'Argument List:', str(sys.argv[1])
