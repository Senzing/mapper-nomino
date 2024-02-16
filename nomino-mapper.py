#! /usr/bin/env python3

import sys
import os
import argparse
import csv
import json
import time
from datetime import datetime
from dateutil.parser import parse as dateparse
import signal
import random
import hashlib

#=========================
class mapper():

    #----------------------------------------
    def __init__(self):

        self.load_reference_data()
        self.stat_pack = {}
        self.record_cache = {}
        self.person_types = ('INDIVIDUAL', 'PERSON', 'P', 'PERSONNE PHYSIQUE', 'PERSONNE MORALE', 'BANNED TERRORIST INDIVIDUAL')

    #----------------------------------------
    def map(self, raw_data, input_row_num = None):

        #--clean values
        for attribute in raw_data:
            raw_data[attribute] = self.clean_value(raw_data[attribute])

        #--place any filters needed here

        #--place any calculations needed here

        record_id = self.compute_record_hash(raw_data, ['Source', 'OriginalID', 'namefull', 'Title','page_URL'])

        raw_type = raw_data.get('type', '').upper()

        if raw_type == 'AIRCRAFT':
            record_type = 'AIRCRAFT'
        elif raw_type  in ('VESSEL', 'SHIP') or raw_data.get('Vess_type'):
            record_type = 'VESSEL'
        elif raw_type in self.person_types or raw_data.get('Person', '0') == '1' or raw_data.get('First'):
            record_type = 'PERSON'
            #if not (raw_type in self.person_types or raw_data.get('Person', '0') == '1'):
            #    input(json.dumps(raw_data, indent=4))
        else:
            record_type = 'ORGANIZATION'

        if record_id not in self.record_cache:
            self.record_cache[record_id] = {'DATA_SOURCE': args.data_source.upper(),
                                            'RECORD_ID': record_id,
                                            'RECORD_TYPE': record_type,
                                            'NAME_LIST': [],
                                            'ADDRESS_LIST': [],
                                            'CONTACT_LIST': [],
                                            'IDENTIFIER_LIST': [],
                                            'ATTRIBUTE_LIST': [],
                                            'OTHER_LIST': []}

        # columnName: riskfeedID
        # 100.0 populated, 100.0 unique
        #      7156229 (1)
        #      7156230 (1)
        #      7156231 (1)
        #      7156232 (1)
        #      7156233 (1)
        #json_data['riskfeedID'] = raw_data.get('riskfeedID']

        # columnName: OriginalID
        # 95.92 populated, 62.39 unique
        #      10923 (3800)
        #      17013 (1872)
        #      10486 (608)
        #      20820 (511)
        #      CAF0010 (400)
        if raw_data.get('OriginalID'):
            self.record_cache[record_id]['OTHER_LIST'].append({'OriginalID': raw_data.get('OriginalID')})

        # columnName: namefull
        # 100.0 populated, 64.12 unique
        #      REVIVAL OF ISLAMIC HERITAGE SOCIETY (3992)
        #      VTB BANK PUBLIC JOINT STOCK COMPANY (1872)
        #      TAMILS REHABILITATION ORGANISATION (609)
        #      AL-OMGY AND BROTHERS MONEY EXCHANGE (512)
        #      AERO CONTINENTE S.A (330)
        if raw_data.get('namefull') and not raw_data.get('First'):
            name_attribute = 'PRIMARY_NAME_FULL' if record_type == 'PERSON' else 'PRIMARY_NAME_ORG'
            self.record_cache[record_id]['NAME_LIST'].append({name_attribute: raw_data.get('namefull')})
        elif raw_data.get('First') or raw_data.get('Middle') or raw_data.get('Last'):
            parsed_name = {}
            # columnName: courtesytitle
            # 0.06 populated, 5.26 unique
            #      Mr (110)
            #      MR (19)
            #      Ms (15)
            #      MS (2)
            #      DR (2)
            parsed_name['PRIMARY_NAME_PREFIX'] = raw_data.get('courtesytitle')

            # columnName: First
            # 56.55 populated, 17.49 unique
            #      MICHAEL (1714)
            #      JOHN (1686)
            #      JAMES (1615)
            #      ROBERT (1611)
            #      DAVID (1414)
            parsed_name['PRIMARY_NAME_FIRST'] = raw_data.get('First')

            # columnName: Middle
            # 34.55 populated, 12.85 unique
            #      A. (2491)
            #      L. (2214)
            #      ANN (2178)
            #      M. (2121)
            #      J. (1849)
            parsed_name['PRIMARY_NAME_MIDDLE'] = raw_data.get('Middle')

            # columnName: Last
            # 58.99 populated, 33.09 unique
            #      SMITH (909)
            #      JOHNSON (788)
            #      WILLIAMS (688)
            #      JONES (662)
            #      BROWN (649)
            parsed_name['PRIMARY_NAME_LAST'] = raw_data.get('Last')

            # columnName: Suffix
            # 1.0 populated, 1.57 unique
            #      JR. (1098)
            #      JR (727)
            #      III (345)
            #      II (165)
            #      SR. (152)
            parsed_name['PRIMARY_NAME_SUFFIX'] = raw_data.get('Suffix')

            self.record_cache[record_id]['NAME_LIST'].append(parsed_name)


        # columnName: Scriptname
        # 1.6 populated, 74.1 unique
        #      خلیل احمد حقانی (33)
        #      حاجی خيرالله و حاجی ستار صرافی (28)
        #      عبد الرحمن ولد العامر (25)
        #      صرافی روشان (20)
        #      حاجى مالك نورزى (18)
        if raw_data.get('Scriptname'):
            self.record_cache[record_id]['NAME_LIST'].append({'NATIVE_NAME_FULL': raw_data.get('Scriptname')})

        # columnName: Title
        # 19.68 populated, 4.79 unique
        #      -0- (46149)
        #      Member of the State Duma of the Federal Assembly of the Russian Federation (454)
        #      Scientific Studies and Research Center Employee (271)
        #      Haji (249)
        #      Maulavi (202)
        if raw_data.get('Title'):
            self.record_cache[record_id]['OTHER_LIST'].append({'Title': raw_data.get('Title')})

        # columnName: marks
        # 14.75 populated, 0.0 unique
        #      -0- (39705)
        #json_data['marks'] = raw_data.get('marks']

        # columnName: Sex
        # 7.85 populated, 0.04 unique
        #      M (7534)
        #      Male (6412)
        #      Masculin (2403)
        #      male (2171)
        #      F (1245)
        if raw_data.get('Sex'):
            self.record_cache[record_id]['ATTRIBUTE_LIST'].append({'GENDER': 'M' if raw_data.get('Sex', '') == 'Masculin' else raw_data.get('Sex')})

        # columnName: Languages
        # 5.06 populated, 0.29 unique
        #      EN (2236)
        #      SV (2206)
        #      RU (1674)
        #      SK (518)
        #      CS (497)
        if raw_data.get('Languages'):
            self.record_cache[record_id]['OTHER_LIST'].append({'Languages': raw_data.get('Languages')})

        # columnName: TIN
        # 0.71 populated, 13.74 unique
        #      (1) 20645897 (2) - (105)
        #      (1) 3219222002.2181558. (2) 2181558 (72)
        #      20227088368 (60)
        #      11010046398 (57)
        #      103-00653129-22 (56)
        if raw_data.get('TIN'):
            self.record_cache[record_id]['IDENTIFIER_LIST'].append({'NATIONAL_ID_NUMBER': raw_data.get('TIN')})

        # columnName: documents
        # 1.62 populated, 43.84 unique
        #      S335026 (73)
        #      (1) A-333602 (2) M110522 (3) R841697 (4) F823692 (5) A501801 (6) K560098 (7) V57865 (8) P537849 (9) A717288 (10) G866537 (11) C-267185 (12) H-123259 (13) G-869537 (14) KC-285901 (72)
        #      (1) D00001184 (2) P04838205 (60)
        #      (1) D00000897 (2) D00004262 (56)
        #      00814L001424 (55)
        if raw_data.get('documents'):
            self.record_cache[record_id]['OTHER_LIST'].append({'documents': raw_data.get('documents')})

        # columnName: POB
        # 6.41 populated, 7.2 unique
        #      Russia (1359)
        #      Syria (1041)
        #      Afghanistan (580)
        #      Iran (401)
        #      (1) Uganda (2) Uganda (3)Uganda (400)
        if raw_data.get('POB'):
            pob_attr = 'PLACE_OF_BIRTH' if record_type == 'PERSON' else 'REGISTRATION_COUNTRY'
            self.record_cache[record_id]['ATTRIBUTE_LIST'].append({pob_attr: raw_data.get('POB')})

        # columnName: DOB
        # 7.84 populated, 32.95 unique
        #      1951 (175)
        #      00/00/1961 (126)
        #      00/00/1977 (125)
        #      00/00/1963 (123)
        #      00/00/1959 (116)
        if raw_data.get('DOB'):
            dob_attr = 'DATE_OF_BIRTH' if record_type == 'PERSON' else 'REGISTRATION_DATE'
            self.record_cache[record_id]['ATTRIBUTE_LIST'].append({dob_attr: raw_data.get('DOB')})

        # columnName: citizenship
        # 5.01 populated, 2.1 unique
        #      Russia (1454)
        #      Syria (1161)
        #      Afghanistan (1121)
        #      Belarus (755)
        #      Iraq (665)
        if raw_data.get('citizenship'):
            self.record_cache[record_id]['ATTRIBUTE_LIST'].append({'CITIZENSHIP': raw_data.get('citizenship')})

        # columnName: phone
        # 0.3 populated, 92.62 unique
        #      8523-8462 to 70 loc. 105/ 8354-09-91 (28)
        #      8523-8462 to 70 loc. 105/835409-91 (8)
        #      Singapore:+65 63922713,Fax: +65 63922716,Malaysia:+60 72211871 (7)
        #      +8 (800) 555-55-50 (4)
        #      +44 0208 895 6910 (3)
        if raw_data.get('phone'):
            self.record_cache[record_id]['CONTACT_LIST'].append({'PHONE_NUMBER': raw_data.get('phone')})

        # columnName: email
        # 0.66 populated, 26.83 unique
        #      helmand_exchange_msp@yahoo.com (243)
        #      (1) info@stepfasteners.com (2) miladjafari@ekolay.net (3) purchase@stepfasteners.com (4) sales@stepfasteners.com (80)
        #      info@hesaco.com (72)
        #      info@ieimil.ir (63)
        #      info@irmig.ir (45)
        if raw_data.get('email'):
            self.record_cache[record_id]['CONTACT_LIST'].append({'EMAIL_ADDRESS': raw_data.get('email')})

        # columnName: website
        # 0.8 populated, 86.8 unique
        #      www.indexgb.com,http://dedydollar.blogspot.sg/2013/10/penawaran-saham-perdana-pra-ipo-www.html,https (5)
        #      www.vangossumconsult.com (3)
        #      http://www.stglimitedasia.com (3)
        #      http://www.aquaintproperty.com/ (3)
        #      http://iconsolutions.eu/about/,http://www.itraonline.com/,http://www.realtyaccess.global/portfolio/ (3)
        if raw_data.get('website'):
            self.record_cache[record_id]['CONTACT_LIST'].append({'WEBSITE_ADDRESS': raw_data.get('website')})

        # columnName: constituancy
        # 0.05 populated, 25.55 unique
        #      410 - Construction (18)
        #      800 - Security (12)
        #      702 - General Services (8)
        #      561 - General Services (7)
        #      854 - General Services (7)
        if raw_data.get('constituancy'):
            self.record_cache[record_id]['OTHER_LIST'].append({'constituancy': raw_data.get('constituancy')})

        # columnName: Image_URL
        # 0.49 populated, 43.42 unique
        #      https://naptip.gov.ng/trafficking-in-persons-register/ (408)
        #      http://kenyalaw.org/kenya_gazette/temp/img/pdfdl.gif (196)
        #      https://www.mha.gov.in/sites/default/files/2023-06/listof54terrorists_22062023.pdf (54)
        #      https://www.mha.gov.in/sites/default/files/2023-03/TERRORIST_ORGANIZATIONS_10032023.pdf (44)
        #      https://www.mha.gov.in/sites/default/files/2023-01/NAMESOFUNLAWFULASSOCIATIONS_20012023.pdf (21)
        if raw_data.get('Image_URL'):
            self.record_cache[record_id]['OTHER_LIST'].append({'Image_URL': raw_data.get('Image_URL')})

        # columnName: page_URL
        # 99.85 populated, 0.13 unique
        #      https://www.sam.gov (148705)
        #      http://www.treasury.gov/resource-center/sanctions/SDN-List/Pages/default.aspx (48579)
        #      http://ec.europa.eu/external_relations/cfsp/sanctions/list/version4/global/global.xml (22052)
        #      http://www.hm-treasury.gov.uk/fin_sanctions_index.htm (17686)
        #      https://www.dfat.gov.au/international-relations/security/sanctions/Pages/consolidated-list (8295)
        if raw_data.get('page_URL'):
            self.record_cache[record_id]['OTHER_LIST'].append({'page_URL': raw_data.get('page_URL')})

        # columnName: Source
        # 99.85 populated, 0.01 unique
        #      System for Award Management Exclusions (148705)
        #      Specially Designated Nationals List (48579)
        #      Consolidated list of persons, groups and entities subject to EU financial sanctions (22052)
        #      HM-Treasury Consolidated list of financial sanctions targets (17686)
        #      DFAT - Consolidated list (8295)
        if raw_data.get('Source'):
            self.record_cache[record_id]['OTHER_LIST'].append({'Source': raw_data.get('Source')})

        # columnName: type
        # 99.54 populated, 0.02 unique
        #      Individual (145851)
        #      -0- (31189)
        #      individual (16196)
        #      Special Entity Designation (13941)
        #      P (13531)
        if raw_data.get('type'):
            self.record_cache[record_id]['OTHER_LIST'].append({'type': raw_data.get('type')})

        # columnName: offense
        # 40.17 populated, 0.59 unique
        #      Z1 (52016)
        #      R (19689)
        #      Z2 (10908)
        #      Z (4397)
        #      03-SDN-01 (4364)
        if raw_data.get('offense'):
            self.record_cache[record_id]['OTHER_LIST'].append({'offense': raw_data.get('offense')})

        # columnName: wantedby
        # 55.3 populated, 0.1 unique
        #      HHS (70787)
        #      OPM (38344)
        #      TREAS-OFAC (25197)
        #      DOJ (2580)
        #      EPA (2198)
        if raw_data.get('wantedby'):
            self.record_cache[record_id]['OTHER_LIST'].append({'wantedby': raw_data.get('wantedby')})

        # columnName: Program
        # 88.48 populated, 0.35 unique
        #      Prohibition/Restriction (117217)
        #      Ineligible (Proceedings Completed) (28424)
        #      SDGT (14105)
        #      UKR (8496)
        #      RUSSIA-EO14024 (5670)
        if raw_data.get('Program'):
            self.record_cache[record_id]['OTHER_LIST'].append({'Program': raw_data.get('Program')})

        # columnName: Legalbasis
        # 69.94 populated, 0.55 unique
        #      Reciprocal (139458)
        #      NonProcurement (9199)
        #      UKR (8496)
        #      TAQA (2655)
        #      1 (2503)
        if raw_data.get('Legalbasis'):
            self.record_cache[record_id]['OTHER_LIST'].append({'Legalbasis': raw_data.get('Legalbasis')})

        # columnName: Listingdate
        # 17.99 populated, 3.58 unique
        #      2022/09/15 00:00:00.000 (1521)
        #      2020/05/29 00:00:00.000 (1437)
        #      2023/09/14 00:00:00.000 (1235)
        #      2023/03/14 00:00:00.000 (938)
        #      2023/07/21 00:00:00.000 (753)
        if raw_data.get('Listingdate'):
            self.record_cache[record_id]['OTHER_LIST'].append({'Listingdate': raw_data.get('Listingdate')})

        # columnName: Call_sign
        # 18.05 populated, 0.38 unique
        #      -0- (48350)
        #      T2EM4 (5)
        #      T2DQ4 (4)
        #      T2EH4 (4)
        #      T2ER4 (4)
        if raw_data.get('Call_sign'):
            self.record_cache[record_id]['CONTACT_LIST'].append({'CALL_SIGN': raw_data.get('Call_sign')})

        # columnName: Vess_type
        # 18.08 populated, 0.09 unique
        #      -0- (47850)
        #      General Cargo (149)
        #      Fishing Vessel (149)
        #      Crude Oil Tanker (129)
        #      Oil tanker (56)
        if raw_data.get('Vess_type'):
            self.record_cache[record_id]['OTHER_LIST'].append({'Vess_type': raw_data.get('Vess_type')})

        # columnName: Tonnage
        # 18.05 populated, 0.09 unique
        #      -0- (48471)
        #      318,000 (10)
        #      317,356 (9)
        #      159,681 (7)
        #      99,144 (6)
        if raw_data.get('Tonnage'):
            self.record_cache[record_id]['OTHER_LIST'].append({'Tonnage': raw_data.get('Tonnage')})

        # columnName: GRT
        # 18.08 populated, 0.19 unique
        #      -0- (48390)
        #      56,068 (15)
        #      163,660 (14)
        #      160,930 (14)
        #      165,000 (10)
        if raw_data.get('GRT'):
            self.record_cache[record_id]['OTHER_LIST'].append({'GRT': raw_data.get('GRT')})

        # columnName: Vess_Flag
        # 18.08 populated, 0.08 unique
        #      -0- (47774)
        #      Iran (230)
        #      China (159)
        #      Russia (152)
        #      North Korea (81)
        if raw_data.get('Vess_Flag'):
            self.record_cache[record_id]['OTHER_LIST'].append({'Vess_Flag': raw_data.get('Vess_Flag')})

        # columnName: Vess_owner
        # 18.08 populated, 0.03 unique
        #      -0- (48573)
        #      Korea Samjong Shipping (14)
        #      Phyongchon Shipping & Marine (11)
        #      Hapjanggang Shipping Corp (10)
        #      Chonmyong Shipping Co (9)
        if raw_data.get('Vess_owner'):
            self.record_cache[record_id]['OTHER_LIST'].append({'Vess_owner': raw_data.get('Vess_owner')})

        # columnName: remarks
        # 44.08 populated, 40.43 unique
        #      Website www.alturath.org. Revival of Islamic Heritage Society Offices Worldwide. (3800)
        #      -0- (2422)
        #      SWIFT/BIC VTBRRUMM; Website www.vtb.com; alt. Website www.vtb.ru; BIK (RU) 044030707; alt. BIK (RU) 044525187; Executive Order 13662 Directive Determination - Subject to Directive 1; Secondary sanctions risk: Ukraine-/Russia-Related Sanctions Regulations, 31 CFR 589.201 and/or 589.209; Organization Established Date 17 Oct 1990; Target Type Financial Institution; Registration ID 1027739609391 (Russia); Tax ID No. 7702070139 (Russia); Government Gazette Number 00032520 (Russia); License 1000 (Russia); Legal Entity Number 253400V1H6ART1UQ0N98 (Russia); For more information on directives, please visit the following link: http://www.treasury.gov/resource-center/sanctions/Programs/Pages/ukraine.aspx#directives. (1872)
        #      ; (1625)
        #      12/31/2999 (852)
        if raw_data.get('remarks'):
            self.record_cache[record_id]['OTHER_LIST'].append({'remarks': raw_data.get('remarks')})

        if raw_data.get('Address') or raw_data.get('City') or raw_data.get('province') or raw_data.get('postcode') or raw_data.get('Country'):
            address_data = {}

            # columnName: Address
            # 27.13 populated, 32.01 unique
            #      -0- (17511)
            #      1 Okhotny Ryad str (405)
            #      RIHS Office (285)
            #      Bashnya Zapad, Kompleks Federatsiya, 12, nab. Presnenskaya (208)
            #      29, Bolshaya Morskaya str. (208)
            if raw_data.get('Address'):
                address_data['ADDR_LINE1'] = raw_data.get('Address')

            # columnName: City
            # 68.17 populated, 9.79 unique
            #      -0- (13509)
            #      MIAMI (3263)
            #      Tehran (1486)
            #      HOUSTON (1188)
            #      MOSCOW (1171)
            if raw_data.get('City'):
                address_data['ADDR_CITY'] = raw_data.get('City')

            # columnName: province
            # 48.98 populated, 0.86 unique
            #      CA (15742)
            #      FL (12859)
            #      TX (10154)
            #      NY (7425)
            #      PA (4805)
            if raw_data.get('province'):
                address_data['ADDR_STATE'] = raw_data.get('province')

            # columnName: postcode
            # 0.75 populated, 17.82 unique
            #      103265 (404)
            #      103426 (158)
            #      283001 (113)
            #      119019 (56)
            #      34940 (31)
            if raw_data.get('postcode'):
                address_data['ADDR_POSTAL_CODE'] = raw_data.get('postcode')

            # columnName: Country
            # 83.17 populated, 0.24 unique
            #      USA (127787)
            #      Russia (12380)
            #      XUN (9150)
            #      Iran (5301)
            #      -0- (4662)
            if raw_data.get('Country'):
                address_data['ADDR_COUNTRY'] = raw_data.get('Country')

            self.record_cache[record_id]['ADDRESS_LIST'].append(address_data)

        # columnName: Address_remarks
        # 20.26 populated, 0.27 unique
        #      -0- (48579)
        #      MYS (1350)
        #      SGP (1044)
        #      NGA (535)
        #      Los Angeles County (399)
        if raw_data.get('Address_remarks'):
            self.record_cache[record_id]['OTHER_LIST'].append({'Address_remarks': raw_data.get('Address_remarks')})

        # columnName: Alias_type
        # 25.1 populated, 0.08 unique
        #      aka (41601)
        #      AKA (7735)
        #      Primary name (5374)
        #      Primary name variation (4729)
        #      fka (2792)
        if raw_data.get('Alias_type'):
            self.record_cache[record_id]['OTHER_LIST'].append({'Alias_type': raw_data.get('Alias_type')})

        # columnName: Alias_name
        # 17.31 populated, 48.8 unique
        #      BANK VNESHNEY TORGOVLI OPEN JOINT STOCK COMPANY (72)
        #      VNESHTORGBANK ROSSII CLOSED JOINT STOCK COMPANY (72)
        #      JSC VTB BANK (72)
        #      BANK VTB OPEN JOINT STOCK COMPANY (72)
        #      BANK VNESHNEY TORGOVLI ROSSIYSKOY FEDERATSII CLOSED JOINT STOCK COMPANY (72)
        if raw_data.get('Alias_name'):
            self.record_cache[record_id]['NAME_LIST'].append({'ALIAS_NAME_FULL': raw_data.get('Alias_name')})

        # columnName: riskcode
        # 100.0 populated, 0.0 unique
        #      WL (269183)
        if raw_data.get('riskcode'):
            self.record_cache[record_id]['OTHER_LIST'].append({'riskcode': raw_data.get('riskcode')})

        # columnName: active
        # 100.0 populated, 0.0 unique
        #      1 (269183)
        if raw_data.get('active'):
            self.record_cache[record_id]['OTHER_LIST'].append({'active': raw_data.get('active')})

        # columnName: timestamp
        # 100.0 populated, 0.09 unique
        #      2024/02/05 23:00:34.000000000 (5177)
        #      2024/02/05 23:00:33.000000000 (5164)
        #      2024/02/05 23:00:31.000000000 (5049)
        #      2024/02/05 23:00:35.000000000 (5032)
        #      2024/02/05 23:00:30.000000000 (4998)
        if raw_data.get('timestamp'):
            self.record_cache[record_id]['OTHER_LIST'].append({'timestamp': raw_data.get('timestamp')})

        # columnName: Person
        # 3.05 populated, 0.02 unique
        #      0 (5676)
        #      1 (2532)
        if raw_data.get('Person'):
            self.record_cache[record_id]['OTHER_LIST'].append({'Person': raw_data.get('Person')})

        #--remove empty attributes and capture the stats
        #json_data = self.remove_empty_tags(json_data)
        #self.capture_mapped_stats(json_data)
        #return json_data

    #----------------------------------------
    def load_reference_data(self):

        #--garabage values
        self.variant_data = {}
        self.variant_data['GARBAGE_VALUES'] = ['NULL', 'NUL', 'N/A', '-0-']

    #-----------------------------------
    def clean_value(self, raw_value):
        if not raw_value:
            return ''
        new_value = ' '.join(str(raw_value).strip().split())
        if new_value.upper() in self.variant_data['GARBAGE_VALUES']: 
            return ''
        return new_value

    #-----------------------------------
    def compute_record_hash(self, target_dict, attr_list = None):
        if attr_list:
            string_to_hash = ''
            for attr_name in sorted(attr_list):
                string_to_hash += (' '.join(str(target_dict[attr_name]).split()).upper() if attr_name in target_dict and target_dict[attr_name] else '') + '|'
        else:           
            string_to_hash = json.dumps(target_dict, sort_keys=True)
        return hashlib.md5(bytes(string_to_hash, 'utf-8')).hexdigest()

    #----------------------------------------
    def format_date(self, raw_date):
        try: 
            return datetime.strftime(dateparse(raw_date), '%Y-%m-%d')
        except: 
            self.update_stat('!INFO', 'BAD_DATE', raw_date)
            return ''

    #----------------------------------------
    def remove_empty_tags(self, d):
        if isinstance(d, dict):
            for  k, v in list(d.items()):
                if v is None or len(str(v).strip()) == 0:
                    del d[k]
                else:
                    self.remove_empty_tags(v)
        if isinstance(d, list):
            for v in d:
                self.remove_empty_tags(v)
        return d

    #----------------------------------------
    def update_stat(self, cat1, cat2, example=None):

        if cat1 not in self.stat_pack:
            self.stat_pack[cat1] = {}
        if cat2 not in self.stat_pack[cat1]:
            self.stat_pack[cat1][cat2] = {}
            self.stat_pack[cat1][cat2]['count'] = 0

        self.stat_pack[cat1][cat2]['count'] += 1
        if example:
            if 'examples' not in self.stat_pack[cat1][cat2]:
                self.stat_pack[cat1][cat2]['examples'] = []
            if example not in self.stat_pack[cat1][cat2]['examples']:
                if len(self.stat_pack[cat1][cat2]['examples']) < 5:
                    self.stat_pack[cat1][cat2]['examples'].append(example)
                else:
                    randomSampleI = random.randint(2, 4)
                    self.stat_pack[cat1][cat2]['examples'][randomSampleI] = example
        return

    #----------------------------------------
    def capture_mapped_stats(self, json_data):

        record_type = json_data.get('RECORD_TYPE', 'UNKNOWN_TYPE')

        for key1 in json_data:
            if type(json_data[key1]) != list:
                self.update_stat(record_type, key1, json_data[key1])
            else:
                for subrecord in json_data[key1]:
                    for key2 in subrecord:
                        self.update_stat(record_type, key2, subrecord[key2])

#----------------------------------------
def signal_handler(signal, frame):
    print('USER INTERUPT! Shutting down ... (please wait)')
    global shut_down
    shut_down = True
    return

#----------------------------------------
if __name__ == "__main__":
    proc_start_time = time.time()
    shut_down = False   
    signal.signal(signal.SIGINT, signal_handler)

    input_file = 'riskcodeWL.csv'
    csv_dialect = 'excel'

    parser = argparse.ArgumentParser()
    parser.add_argument('-i', '--input_file', dest='input_file', default = input_file, help='the name of the input file')
    parser.add_argument('-o', '--output_file', dest='output_file', help='the name of the output file')
    parser.add_argument('-l', '--log_file', dest='log_file', help='optional name of the statistics log file')
    parser.add_argument('-d', '--data_source', dest='data_source', default='NOMINO', help='the data source code to use, default="NOMINO"')
    args = parser.parse_args()

    if not args.input_file or not os.path.exists(args.input_file):
        print('\nPlease supply a valid input file name on the command line\n')
        sys.exit(1)
    if not args.output_file:
        print('\nPlease supply a valid output file name on the command line\n') 
        sys.exit(1)

    input_file_handle = open(args.input_file, 'r')
    output_file_handle = open(args.output_file, 'w', encoding='utf-8')
    mapper = mapper()

    input_row_count = 0
    for input_row in csv.DictReader(input_file_handle, dialect=csv_dialect):
        mapper.map(input_row, input_row_count)
        input_row_count += 1
        if input_row_count % 1000 == 0:
            print(f'{input_row_count} rows read')
        if shut_down:
            break

    output_row_count = 0
    if not shut_down:
        for record_id in mapper.record_cache.keys():
            json_data = mapper.record_cache[record_id]
            mapper.capture_mapped_stats(json_data)
            output_file_handle.write(json.dumps(json_data) + '\n')
            output_row_count += 1

            if output_row_count % 1000 == 0:
                print(f'{output_row_count} rows written')
            if shut_down:
                break

    elapsed_mins = round((time.time() - proc_start_time) / 60, 1)
    run_status = ('completed in' if not shut_down else 'aborted after') + ' %s minutes' % elapsed_mins
    print(f'{input_row_count} rows read, {output_row_count} rows written, {run_status}\n')

    output_file_handle.close()
    input_file_handle.close()

    #--write statistics file
    if args.log_file: 
        with open(args.log_file, 'w') as outfile:
            json.dump(mapper.stat_pack, outfile, indent=4, sort_keys = True)
        print('Mapping stats written to %s\n' % args.log_file)


    sys.exit(0)

