#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Jan  7 11:53:39 2020

@author: ehsannegahbani
"""
# importing the requests library 
import requests 
import pandas as pd
import time


# List of watersheds of interest
# The watershed identifiers, or hydrologic unit codes (HUC) of interest iclude:
HUC_list = ['11010001', '11010002',
       '11010003', '11010004',
      '11010005', '11010006',
      '11010007', '11010008',
      '11010009', '11010010',
      '11010011', '11010012',
      '11010013', '11010014',
      '08020301', '08020302',
      '08020303', '08020304']

year = str(2019)

data_dict = {'Year':[],
                 'HUC_8':[],
                'NPDES_ID':[],
                'Facility Name':[],
                'Latitude':[],
                'Longitude':[],
                'Facility Type':[],
                'Permit Type':[],
                'Permit Effective Date':[],
                'Permit Expiration Date':[],
                'Major/Non-Major Indicator':[],
                'Approved Pretreatment Program':[],
                'County':[],
                'Facility Design Flow (Permit Application) (MGD)':[],
                'Average Facility Flow (MGD)':[],
                '4-Digit SIC Code':[],
                 'Pollutant':[],
                 'Parameter Code':[],
                 'Total Pounds (lb/yr)':[],
                 'Max Allowable Load (lb/yr)':[],
                 'Load Over Limit':[],
                 'Total TWPE (lb-eq/yr)':[],
                 'Max Allowable TWPE (lb-eq/yr)':[],
                 'TWPE Over Limit':[],
                 'Contains Potential Outliers?':[],
                 'Number of Exceedances':[],
                 'QcReviewPounds':[],
                 'QcReviewTwpe':[]
                }

# Main:

# api-endpoint 
# URL = "http://echo.epa.gov/tools/web-services/loading-tool/dmr_rest_services.get_dmr_loadings"
URL = 'https://ofmpub.epa.gov/echo/dmr_rest_services.get_dmr_loadings'  
HUC_cnt =1
for HUC in HUC_list:
    print('HUC={} ({}/{})'.format(HUC, HUC_cnt, len(HUC_list)))
    # defining a params dict for the parameters to be sent to the API 
    PARAMS = {'output':'JSON',
             'p_year':year,
             'p_huc':HUC,
    #           'pageno' : 3,
             'responseset': 5, # Number of rows to be retreived
             'download':'TopFacilityPounds' # This downloads each facility 5 times (1 per top pollutant)
             } # There might be a better API that does not do this repetititev.
    
      
    # sending get request and saving the response as response object 
    r = requests.get(url = URL, params = PARAMS) 
      
    # extracting data in json format 
    data = r.json() 
    
    facility_numbers = data['Results']['FacilityCounts']['AllFacs']
    retrived_facility_numbers = len(data['Results']['TopFacilityPounds'])
    print('Total number of unique facilities in this waterbody: {}'.format(facility_numbers))
    print('Number of retirved facilities (non unique) in this call: {}'.format(retrived_facility_numbers))
    if facility_numbers != retrived_facility_numbers:
        print('==='*30)
        print('WARNING: {} non-unique out of {} unique facilities were retrived!'.format(retrived_facility_numbers, facility_numbers))
        print('==='*30)
    
    facilities = data['Results']['TopFacilityPounds']
    #facilities_stats = data['Results']['TopFacilityPounds']
    Success_flag = facility_counts = data['Results']['Message']
    
    facility_loading_URL = 'https://ofmpub.epa.gov/echo/dmr_rest_services.get_facility_report'  
        # defining a params dict for the parameters to be sent to the API 
        
    facility_id_list = list() # check and eliminate the repeating facilities
    
    for i in range(len(facilities)):
        if  (facilities[i]['ExternalPermitNmbr'] in facility_id_list)==False:
            facility_id_list.append(facilities[i]['ExternalPermitNmbr'])
            
            # only retrieve the data for facility if it is not retrieved before (based on the unique permit id number)
            PARAMS = {'output':'JSON',
                     'p_permit_id':facilities[i]['ExternalPermitNmbr'],
                     'p_year':year,
                     } 
            status_code = 0
            j=1
            # deal with unsuccessfull requests
            
            while status_code==0:
                try:
                    # sending get request and saving the response as response object 
                    facility_r = requests.get(url = facility_loading_URL, params = PARAMS) 
                    status_code = 1
    #                 print('Succ')
                except requests.exceptions.ConnectionError:
            # Does it ever gets here??????
                    status_code = 0
                    print('attempt {}, connection refused!'.format(j))
                    j+=1
                    facility_r.status_code='Connection refused'
            # extracting data in json format 
            facility_data = facility_r.json() 
            #         data_dict['Pollutants'].append(facility_data['Results']['PollutantLoads'])
            
            # Is there any better way than using for loop?
            #print(facility_data['Results'])
            for x in facility_data['Results']['PollutantLoads']:
                #print('x={}'.format(x))
                data_dict['Year'].append(year)
                data_dict['HUC_8'].append(HUC)
                data_dict['NPDES_ID'].append(facilities[i]['ExternalPermitNmbr'])
                data_dict['Facility Name'].append(facilities[i]['FacilityName']+', '+facilities[i]['City']+', '+facility_data['Results']['FacilityInfo']['FacZip'])
                data_dict['Latitude'].append(facilities[i]['GeocodeLatitude'])
                data_dict['Longitude'].append(facilities[i]['GeocodeLongitude'])
                data_dict['Major/Non-Major Indicator'].append(facilities[i]['MajorMinorStatusFlag'])
                data_dict['County'].append(facilities[i]['CountyName'])
                
                data_dict['Facility Type'].append(facility_data['Results']['FacilityInfo']['FacilityType'])
                data_dict['Permit Type'].append(facility_data['Results']['FacilityInfo']['PermitType'])
                data_dict['Permit Effective Date'].append(facility_data['Results']['FacilityInfo']['PermitEffectiveDate'])
                data_dict['Permit Expiration Date'].append(facility_data['Results']['FacilityInfo']['PermitExpirationDate'])
                data_dict['Approved Pretreatment Program'].append(facility_data['Results']['FacilityInfo']['PretreatProgram'])
                data_dict['Facility Design Flow (Permit Application) (MGD)'].append(facility_data['Results']['FacilityInfo']['FacilityDesignFlow'])
                data_dict['Average Facility Flow (MGD)'].append(facility_data['Results']['FacilityInfo']['AvgFacilityFlow'])
                data_dict['4-Digit SIC Code'].append(facility_data['Results']['FacilityInfo']['SicCode'])
             
                data_dict['Pollutant'].append(x['PollutantName'])
                data_dict['Parameter Code'].append(x['ParameterCode'])
                data_dict['Total Pounds (lb/yr)'].append(x['TotalPounds'])
                data_dict['Max Allowable Load (lb/yr)'].append(x['MaxAllowablePounds'])
                data_dict['Load Over Limit'].append(x['LoadOverLimit'])
                data_dict['Total TWPE (lb-eq/yr)'].append(x['TotalTwpe'])
                data_dict['Max Allowable TWPE (lb-eq/yr)'].append(x['MaxAllowableTwpe'])
                data_dict['TWPE Over Limit'].append(x['LoadOverLimitTwpe'])
                data_dict['Contains Potential Outliers?'].append(x['QcFlag'])
                data_dict['Number of Exceedances'].append(x['ExceedanceCount'])
                data_dict['QcReviewPounds'].append(x['QcReviewPounds'])
                data_dict['QcReviewTwpe'].append(x['QcReviewTwpe'])
    
    
            print('{}/{}, {},     facility name: {}'.format(i+1, len(facilities),
                                                            facility_data['Results']['Message'],
                                                            facilities[i]['FacilityName']))
            time.sleep(0.5) # pause between API calls
    HUC_cnt+=1

        

df=pd.DataFrame(data_dict)
output_file = 'output.csv'
pd.DataFrame.to_csv(df, output_file, sep=',', index=False)        
       
  

