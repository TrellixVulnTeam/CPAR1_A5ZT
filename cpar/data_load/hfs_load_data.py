import os
from datetime import datetime
from dbconnect import dbconnect


file_paths = './output_data'
ls = os.listdir(file_paths)

info_dict = {'path': file_paths,
             'adjustment':'adjustedclaimextractuick1.out',
             'main_claims':'claim_finaluick1.out',
             'nips':'servicenips_finaluick1.out',
             'pharmacy':'pharmacy_finaluick1.out',
             'procedure':'serviceproc_finaluick1.out',
             'recipient_flags':'recipientflags_final_uick1.out',
             'revenue':'servicerev_finaluick1.out',
             'compound_drug':'servicepharmndc_finaluick1.out',
             'immunization':'cornerstone_finaluick1.out',
             'diagnosis':'servicediag_finaluick1.out',
             'institutional':'serviceinst_finaluick1.out',
             'lead':'lead_finaluick1.out',
             'ending':'\n\n'}

info_dict['load_date'] = '{:%Y-%m-%d}'.format(datetime.today())

yes_no = False
while yes_no != True:
    yes_no = input('Is HFS the Data Source (Y/N)? ')
    if yes_no == 'Y':
        info_dict['DataSource'] = 'HFS'
        yes_no = True
    elif yes_no == 'N':
        print('\n\nWarning!! Your probably wrong!!\n\n')
        data_source = input('Enter data source: ')
        verify = input("Is '{}' the correct Data Source (Y/N)? ".format(data_source))
        if verify == 'Y':
            info_dict['DataSource'] = data_source
            yes_no = True
        else:
            continue
    else:
        print('Improper input try again')

print()

yes_no = False
while yes_no != True:
    yes_no = input('Is the Database CHECK_CPAR2 (Y/N)? ')
    if yes_no == 'Y':
        info_dict['db'] = 'CHECK_CPAR2'
        yes_no = True
    elif yes_no == 'N':
        db_name = input('Enter DB Name: ')
        verify = input("Is '{}' the correct DB (Y/N)? ".format(db_name))
        if verify == 'Y':
            info_dict['db'] = db_name
            yes_no = True
        else:
            continue
    else:
        print('Improper input try again')

connection = dbconnect.DatabaseConnect(info_dict['db'])
#gets last inserts release and adds one
current_releasenum = connection.query('SELECT MAX(ReleaseNum) from pat_info_demo').values[0][0] + 1

print()

yes_no = False
while yes_no != True:
    yes_no = input('Is {} the correct Release Number  (Y/N)? '.format(current_releasenum))
    if yes_no == 'Y':
        info_dict['ReleaseNum'] = str(current_releasenum)
        info_dict['Cumulative_ReleaseNum'] = info_dict['ReleaseNum'][2:]
        yes_no = True
    elif yes_no == 'N':
        print('\n\nWarning!! Your probably wrong!!\n\n')
        input_release = input('Enter ReleaseNum: ')
        assert int(input_release) != ValueError
        verify = input("Is'{}' the correct Release Number (Y/N)? ".format(input_release))
        if verify == 'Y':
            info_dict['ReleaseNum'] = input_release
            info_dict['Cumulative_ReleaseNum'] = info_dict['ReleaseNum'][2:]
            yes_no = True
        else:
            continue
    else:
        print('Improper input try again')



def renew_script(file_name, start_string=None):
    '''Rewrites a file if it exists and will add a header to the file with start_string'''
    try:
        os.remove(file_name)
    except:
        pass
    finally:
        if start_string != None:
            text_file = open(file_name, "a")
            text_file.write(start_string)
            text_file.close()

adjustment_table = """LOAD DATA LOCAL INFILE '{path}/{adjustment}'
INTO TABLE {db}.trc_hfs_adjustments
(@row)
SET
    DCN = TRIM(SUBSTR(@row,1,15)),
    ServiceLineNbr = TRIM(SUBSTR(@row,16,2)),
    RecipientID = TRIM(SUBSTR(@row,18,9)),
    AdjudicatedDt = str_to_date(TRIM(SUBSTR(@row,27,10)), '%Y-%m-%d'),
    CorrectedNetLiabilityAmt = nullif(TRIM(SUBSTR(@row,37,11)),''),
    DeltaNetLiabilityAmt = nullif(TRIM(SUBSTR(@row,48,11)),''),
    VoidInd = TRIM(SUBSTR(@row,59,1)),
    DataSource = '{DataSource}',
    ReleaseNum = {ReleaseNum},
    CumReleaseNum = {Cumulative_ReleaseNum};{ending}""".format(**info_dict)

main_claims_table = """LOAD DATA LOCAL INFILE '{path}/{main_claims}'
INTO TABLE {db}.trc_hfs_main_claims
(@row)
SET
    DCN = TRIM(SUBSTR(@row,1,15)),
    ServiceLineNbr = TRIM(SUBSTR(@row,16,2)),
    RejectionStatusCd = TRIM(SUBSTR(@row,18,1)),
    RecipientID = TRIM(SUBSTR(@row,19,9)),
    AdjudicatedDt = str_to_date(TRIM(SUBSTR(@row,28,10)), '%Y-%m-%d'),
    ServiceFromDt = str_to_date(TRIM(SUBSTR(@row,38,10)), '%Y-%m-%d'),
    ServiceThruDt = str_to_date(TRIM(SUBSTR(@row,48,10)), '%Y-%m-%d'),
    CatgofServiceCd = TRIM(SUBSTR(@row,58,3)),
    RecordIDCd = TRIM(SUBSTR(@row,61,1)),
    ProviderID = TRIM(SUBSTR(@row,62,12)),
    ProviderTypeCd = TRIM(SUBSTR(@row,74,3)),
    DataTypeCd = TRIM(SUBSTR(@row,77,1)),
    DocumentCd = TRIM(SUBSTR(@row,78,2)),
    PayeeID = TRIM(SUBSTR(@row,80,16)),
    PriorApprovalCd = TRIM(SUBSTR(@row,96,1)),
    ProviderNPI = TRIM(SUBSTR(@row,97,10)),
    EncounterPriceAmt = nullif(TRIM(SUBSTR(@row,107,11)),''),
    NetLiabilityAmt = nullif(TRIM(SUBSTR(@row,118,11)),''),
    MedicareBillProviderTaxonomy = TRIM(SUBSTR(@row,129,10)),
    ProviderTaxonomy = TRIM(SUBSTR(@row,139,10)),
    ProviderChargeAmt = nullif(TRIM(SUBSTR(@row,149,11)),''),
    CopayAmt = nullif(TRIM(SUBSTR(@row,160,11)),''),
    DataSource = '{DataSource}',
    ReleaseNum = {ReleaseNum},
    CumReleaseNum = {Cumulative_ReleaseNum};{ending}""".format(**info_dict)

immunization_table = """LOAD DATA LOCAL INFILE '{path}/{immunization}'
INTO TABLE {db}.trc_hfs_cornerstone_immunization
(@row)
SET
    RecipientID = TRIM(SUBSTR(@row,1,9)),
    ImmnDt = str_to_date(TRIM(SUBSTR(@row,10,10)), '%Y-%m-%d'),
    ImmnTyp = TRIM(SUBSTR(@row,20,4)),
    ImunzTypDesc = TRIM(SUBSTR(@row,24,40)),
    DataSource = '{DataSource}',
    ReleaseNum = {ReleaseNum},
    CumReleaseNum = {Cumulative_ReleaseNum};{ending}""".format(**info_dict)

lead_table = """LOAD DATA LOCAL INFILE '{path}/{lead}'
INTO TABLE {db}.trc_hfs_lead
(@row)
SET
    RecipientID = TRIM(SUBSTR(@row,1,9)),
    LabNumber = TRIM(SUBSTR(@row,10,16)),
    CollectedDate = str_to_date(TRIM(SUBSTR(@row,26,10)), '%Y-%m-%d'),
    BirthDate = str_to_date(TRIM(SUBSTR(@row,36,10)), '%Y-%m-%d'),
    TestResult = TRIM(SUBSTR(@row,46,3)),
    TestType = TRIM(SUBSTR(@row,49,1)),
    ConfirmLevel = TRIM(SUBSTR(@row,50,3)),
    ConfirmDate = str_to_date(TRIM(SUBSTR(@row,53,10)), '%Y-%m-%d'),
    DataSource = '{DataSource}',
    ReleaseNum = {ReleaseNum},
    CumReleaseNum = {Cumulative_ReleaseNum};{ending}""".format(**info_dict)

pharmacy_table = """LOAD DATA LOCAL INFILE '{path}/{pharmacy}'
INTO TABLE {db}.trc_hfs_pharmacy
(@row)
SET
    DCN = TRIM(SUBSTR(@row,1,15)),
    ServiceLineNbr = TRIM(SUBSTR(@row,16,2)),
    RecipientID = TRIM(SUBSTR(@row,18,9)),
    AdjudicatedDt = str_to_date(TRIM(SUBSTR(@row,27,10)), '%Y-%m-%d'),
    ServiceFromDt = str_to_date(TRIM(SUBSTR(@row,37,10)), '%Y-%m-%d'),
    CatgofServiceCd = TRIM(SUBSTR(@row,47,3)),
    RecordIDCd = TRIM(SUBSTR(@row,50,1)),
    ProviderID = TRIM(SUBSTR(@row,51,12)),
    ProviderTypeCd = TRIM(SUBSTR(@row,63,3)),
    DataTypeCd = TRIM(SUBSTR(@row,66,1)),
    DocumentCd = TRIM(SUBSTR(@row,67,2)),
    PayeeID = TRIM(SUBSTR(@row,69,16)),
    PriorApprovalCd = TRIM(SUBSTR(@row,85,1)),
    NationalDrugCd = TRIM(SUBSTR(@row,86,11)),
    DrugDaysSupplyNbr = nullif(TRIM(SUBSTR(@row,97,3)),''),
    DrugQuanAllow = nullif(TRIM(SUBSTR(@row,100,10)),''),
    DrugSpecificTherapeuticClassCd = TRIM(SUBSTR(@row,110,3)),
    PrimaryCareProviderID = TRIM(SUBSTR(@row,113,12)),
    ProviderNPI = TRIM(SUBSTR(@row,125,10)),
    PrescribingPractitionerId = TRIM(SUBSTR(@row,135,12)),
    PrescriptionNbr = TRIM(SUBSTR(@row,147,12)),
    CompoundCd = TRIM(SUBSTR(@row,159,1)),
    RefillNbr = TRIM(SUBSTR(@row,160,2)),
    NbrRefillsAuth = nullif(TRIM(SUBSTR(@row,162,2)),''),
    DrugDAWCd = TRIM(SUBSTR(@row,164,1)),
    PrescriptionDt = str_to_date(TRIM(SUBSTR(@row,165,10)), '%Y-%m-%d'),
    PrescribingLastName = TRIM(SUBSTR(@row,175,15)),
    LabelName = TRIM(SUBSTR(@row,190,30)),
    GenericCdNbr = TRIM(SUBSTR(@row,220,5)),
    DrugStrengthDesc = TRIM(SUBSTR(@row,225,10)),
    GenericInd = TRIM(SUBSTR(@row,235,1)),
    GenericSequenceNbr = TRIM(SUBSTR(@row,236,6)),
    EncounterPriceAmt = nullif(TRIM(SUBSTR(@row,242,11)),''),
    NetLiabilityAmt = nullif(TRIM(SUBSTR(@row,253,11)),''),
    CopayAmt = nullif(TRIM(SUBSTR(@row,264,11)),''),
    DataSource = '{DataSource}',
    ReleaseNum = {ReleaseNum},
    CumReleaseNum = {Cumulative_ReleaseNum};{ending}""".format(**info_dict)

recipient_flags_table = """LOAD DATA LOCAL INFILE '{path}/{recipient_flags}'
INTO TABLE {db}.trc_hfs_recipient_flags
(@row)
SET
    RecipientID = TRIM(SUBSTR(@row,1,9)),
    RecipientFlagCd = TRIM(SUBSTR(@row,10,2)),
    DataSource = '{DataSource}',
    ReleaseNum = {ReleaseNum},
    CumReleaseNum = {Cumulative_ReleaseNum};{ending}""".format(**info_dict)

diagnosis_table = """LOAD DATA LOCAL INFILE '{path}/{diagnosis}'
INTO TABLE {db}.trc_hfs_diagnosis
(@row)
SET
    DCN = TRIM(SUBSTR(@row,1,15)),
    ServiceLineNbr = TRIM(SUBSTR(@row,16,2)),
    RejectionStatusCd = TRIM(SUBSTR(@row,18,1)),
    RecipientID = TRIM(SUBSTR(@row,19,9)),
    AdjudicatedDt = str_to_date(TRIM(SUBSTR(@row,28,10)), '%Y-%m-%d'),
    DiagCd = TRIM(SUBSTR(@row,38,8)),
    PrimaryDiagInd = TRIM(SUBSTR(@row,46,1)),
    TraumaInd = TRIM(SUBSTR(@row,47,1)),
    DiagPrefixCd = TRIM(SUBSTR(@row,48,1)),
    POAClaimCd = TRIM(SUBSTR(@row,49,1)),
    ICDVersion = TRIM(SUBSTR(@row,50,2)),
    DataSource = '{DataSource}',
    ReleaseNum = {ReleaseNum},
    CumReleaseNum = {Cumulative_ReleaseNum};{ending}""".format(**info_dict)

institutional_table = """LOAD DATA LOCAL INFILE '{path}/{institutional}'
INTO TABLE {db}.trc_hfs_institutional
(@row)
SET
    DCN = TRIM(SUBSTR(@row,1,15)),
    ServiceLineNbr = TRIM(SUBSTR(@row,16,2)),
    RejectionStatusCd = TRIM(SUBSTR(@row,18,1)),
    RecipientID = TRIM(SUBSTR(@row,19,9)),
    AdjudicatedDt = str_to_date(TRIM(SUBSTR(@row,28,10)), '%Y-%m-%d'),
    BillTypeFrequencyCd = TRIM(SUBSTR(@row,38,1)),
    AdmissionSourceCd = TRIM(SUBSTR(@row,39,2)),
    AdmissionTypeCd = TRIM(SUBSTR(@row,41,1)),
    DRGGroupCd = TRIM(SUBSTR(@row,42,3)),
    PricingCd = TRIM(SUBSTR(@row,45,1)),
    AdmissionDt = str_to_date(TRIM(SUBSTR(@row,46,10)), '%Y-%m-%d'),
    DischargeDt = str_to_date(TRIM(SUBSTR(@row,56,10)), '%Y-%m-%d'),
    PatientStatusCd = TRIM(SUBSTR(@row,66,2)),
    ProviderDRGAssignedCd = TRIM(SUBSTR(@row,68,7)),
    UBTypeofBillCd = TRIM(SUBSTR(@row,75,3)),
    OutPatientAPLGrp = TRIM(SUBSTR(@row,78,2)),
    APLProcGroupCd = TRIM(SUBSTR(@row,80,5)),
    GrouperVersionNbr = TRIM(SUBSTR(@row,85,3)),
    SOICd = TRIM(SUBSTR(@row,88,1)),
    InpatientAdmissions = nullif(TRIM(SUBSTR(@row,89,3)),''),
    CoveredDays = nullif(TRIM(SUBSTR(@row,92,5)),''),
    AdmissionDiagCd = TRIM(SUBSTR(@row,97,8)),
    ICDVersion = TRIM(SUBSTR(@row,105,2)),
    DataSource = '{DataSource}',
    ReleaseNum = {ReleaseNum},
    CumReleaseNum = {Cumulative_ReleaseNum};{ending}""".format(**info_dict)

nips_table = """LOAD DATA LOCAL INFILE '{path}/{nips}'
INTO TABLE {db}.trc_hfs_nips
(@row)
SET
    DCN = TRIM(SUBSTR(@row,1,15)),
    ServiceLineNbr = TRIM(SUBSTR(@row,16,2)),
    RejectionStatusCd = TRIM(SUBSTR(@row,18,1)),
    RecipientID = TRIM(SUBSTR(@row,19,9)),
    AdjudicatedDt = str_to_date(TRIM(SUBSTR(@row,28,10)), '%Y-%m-%d'),
    PlaceOfServiceCd = TRIM(SUBSTR(@row,38,2)),
    ReferringPractitionerId = TRIM(SUBSTR(@row,40,12)),
    OriginatingPlaceCd = TRIM(SUBSTR(@row,52,3)),
    DestinationPlaceCd = TRIM(SUBSTR(@row,55,3)),
    AllowedUnitsQuan = nullif(TRIM(SUBSTR(@row,58,7)),''),
    TotalUnitsQuan = nullif(TRIM(SUBSTR(@row,65,10)),''),
    SpecialPhysicianNPI = TRIM(SUBSTR(@row,75,10)),
    SeqLineNbr = TRIM(SUBSTR(@row,85,2)),
    DataSource = '{DataSource}',
    ReleaseNum = {ReleaseNum},
    CumReleaseNum = {Cumulative_ReleaseNum};{ending}""".format(**info_dict)

compound_drug_table = """LOAD DATA LOCAL INFILE '{path}/{compound_drug}'
INTO TABLE {db}.trc_hfs_compound_drugs_detail
(@row)
SET
    DCN = TRIM(SUBSTR(@row,1,15)),
    ServiceLineNbr = TRIM(SUBSTR(@row,16,2)),
    RecipientID = TRIM(SUBSTR(@row,18,9)),
    AdjudicatedDt = str_to_date(TRIM(SUBSTR(@row,27,10)), '%Y-%m-%d'),
    NationalDrugCd = TRIM(SUBSTR(@row,37,11)),
    CompoundDispUnitCd = TRIM(SUBSTR(@row,48,1)),
    CompoundDosageFormCd = TRIM(SUBSTR(@row,49,2)),
    IngrQuan = nullif(TRIM(SUBSTR(@row,51,10)),''),
    DataSource = '{DataSource}',
    ReleaseNum = {ReleaseNum},
    CumReleaseNum = {Cumulative_ReleaseNum};{ending}""".format(**info_dict)

procedure_table = """LOAD DATA LOCAL INFILE '{path}/{procedure}'
INTO TABLE {db}.trc_hfs_procedure
(@row)
SET
    DCN = TRIM(SUBSTR(@row,1,15)),
    ServiceLineNbr = TRIM(SUBSTR(@row,16,2)),
    RejectionStatusCd = TRIM(SUBSTR(@row,18,1)),
    RecipientID = TRIM(SUBSTR(@row,19,9)),
    AdjudicatedDt = str_to_date(TRIM(SUBSTR(@row,28,10)), '%Y-%m-%d'),
    ProcCd = TRIM(SUBSTR(@row,38,8)),
    ProcPrefixCd = TRIM(SUBSTR(@row,46,1)),
    ProcDt = str_to_date(TRIM(SUBSTR(@row,47,10)), '%Y-%m-%d'),
    PrimaryProcInd = TRIM(SUBSTR(@row,57,1)),
    ProcModifierCd1 = TRIM(SUBSTR(@row,58,2)),
    ProcModifierCd2 = TRIM(SUBSTR(@row,60,2)),
    ProcModifierCd3 = TRIM(SUBSTR(@row,62,2)),
    ProcModifierCd4 = TRIM(SUBSTR(@row,64,2)),
    ICDVersion = TRIM(SUBSTR(@row,66,2)),
    SeqLineNbr = TRIM(SUBSTR(@row,68,2)),
    DataSource = '{DataSource}',
    ReleaseNum = {ReleaseNum},
    CumReleaseNum = {Cumulative_ReleaseNum};{ending}""".format(**info_dict)


revenue_table = """LOAD DATA LOCAL INFILE '{path}/{revenue}'
INTO TABLE {db}.trc_hfs_revenue_codes
(@row)
SET
    DCN = TRIM(SUBSTR(@row,1,15)),
    ServiceLineNbr = TRIM(SUBSTR(@row,16,2)),
    RejectionStatusCd = TRIM(SUBSTR(@row,18,1)),
    RecipientID = TRIM(SUBSTR(@row,19,9)),
    AdjudicatedDt = str_to_date(TRIM(SUBSTR(@row,28,10)), '%Y-%m-%d'),
    RevenueCd = TRIM(SUBSTR(@row,38,4)),
    RevenueHCPCSCd = TRIM(SUBSTR(@row,42,8)),
    RevenueHCPCSMod1Cd = TRIM(SUBSTR(@row,50,2)),
    RevenueHCPCSMod2Cd = TRIM(SUBSTR(@row,52,2)),
    RevenueHCPCSMod3Cd = TRIM(SUBSTR(@row,54,2)),
    RevenueHCPCSMod4Cd = TRIM(SUBSTR(@row,56,2)),
    NDCNumber1 = TRIM(SUBSTR(@row,58,11)),
    NDCQuantity1 = nullif(TRIM(SUBSTR(@row,69,11)),''),
    NDCNumber2 = TRIM(SUBSTR(@row,80,11)),
    NDCQuantity2 = nullif(TRIM(SUBSTR(@row,91,11)),''),
    NDCNumber3 = TRIM(SUBSTR(@row,102,11)),
    NDCQuantity3 = nullif(TRIM(SUBSTR(@row,113,11)),''),
    RevenueNonCoveredChargeAmt = nullif(TRIM(SUBSTR(@row,124,11)),''),
    RevenueTotalChargeAmt = nullif(TRIM(SUBSTR(@row,135,11)),''),
    SeqLineNbr = TRIM(SUBSTR(@row,146,3)),
    EAPGCd = TRIM(SUBSTR(@row,149,5)),
    EAPGTypeCd = TRIM(SUBSTR(@row,154,2)),
    EAPGCatgCd = TRIM(SUBSTR(@row,156,2)),
    DataSource = '{DataSource}',
    ReleaseNum = {ReleaseNum},
    CumReleaseNum = {Cumulative_ReleaseNum};{ending}""".format(**info_dict)

sql_str_list = [adjustment_table, main_claims_table, nips_table,
                pharmacy_table, procedure_table, recipient_flags_table,
                revenue_table, compound_drug_table, immunization_table,
                diagnosis_table, institutional_table, lead_table]

mysql_script_name = 'Load_Data_to_DB_ReleaseNum_{ReleaseNum}.sql'.format(**info_dict)

renew_script(mysql_script_name)

for i in sql_str_list:
    text_file = open(mysql_script_name, "a")
    text_file.write(i)
    text_file.close()


#writes sql script that puts load information into table
insert_info_script_name = 'Load_info_{ReleaseNum}.sql'.format(**info_dict)
delete_info_script_name = 'Delete_Release_Info_{ReleaseNum}.sql'.format(**info_dict)

renew_script(insert_info_script_name,"USE {db};\n".format(**info_dict))
renew_script(delete_info_script_name,"USE {db};\n".format(**info_dict))


db_tables = ['tsc_hfs_adjustments', 'tsc_hfs_diagnosis', 'tsc_hfs_institutional', 'tsc_hfs_main_claims',
             'tsc_hfs_main_claims_new', 'tsc_hfs_nips', 'tsc_hfs_pharmacy', 'tsc_hfs_procedure', 'tsc_hfs_revenue_codes']


for table in db_tables:
    insert_str = """INSERT INTO tum_hfs_load_count_info(tablename, releasenum,
    cumreleasenum, loaddate, count) select '{table}' as tablename, {ReleaseNum} as releasenum,
    {Cumulative_ReleaseNum} as cumreleasenum, '{load_date}' as loaddate,
    (select count(*) from trc_hfs_adjustments
    where cumreleasenum  = {Cumulative_ReleaseNum}) as count;\n\n""".format(table=table,**info_dict)

    delete_str = """DELETE FROM {table} WHERE datasource = '{DataSource}' AND releasenum = {ReleaseNum};\n""".format(table=table,**info_dict)

    insert_text_file = open(insert_info_script_name, "a")
    insert_text_file.write(insert_str)
    insert_text_file.close()

    delete_text_file = open(delete_info_script_name, "a")
    delete_text_file.write(delete_str)
    delete_text_file.close()
