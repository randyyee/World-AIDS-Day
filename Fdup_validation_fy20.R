# this code was used in FY 20 to extract FY20 (COP19) APR results and FY21 (COP20) targets from Genie extracts
# or MSDs, apply the deduplication algorithm, and output for upload into the PowerApp and a Power BI report
# Authors: Imran Mujawar & Andrea Stewart (ouo8@cdc.gov)
# To run, you will need to update the lines with the filepaths below and make sure the sub-folders are created
# Date last updated: 7/6/2021

library(tidyverse)
library(vroom)

# 
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# ============= Functions used in code ~~~~~~~===============
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

# Creating the 'not in' function
`%ni%` <- Negate(`%in%`) 

#filepath where the Genie/MSDs are saved
#RawDataPath="C:/Users/ouo8/CDC/CPME - Country_Dashboards/FY20/Q4/MSDs/Preclean/Postfrozen/"
RawDataPath="C:/Users/pcx5/Downloads/"

#string for keeping track of the time period that the data were downloaded (i.e. prefrozen, postfrozen, postclean, etc)
timeperiod="FY21Q4_preclean"

#output path - make sure this folder is created, along with a sub-folder called "Site Dedups"
#OutPath="C:/Users/ouo8/OneDrive - CDC/HIDMSB/WAD/WAD 2020/Output/postfrozen_preclean/"
OutPath="C:/Users/pcx5/Downloads/"

#set this to "1" if LAB_PTCQI data are available (i.e. it is Q4 data)
labdata=1

setwd(OutPath)
# Getting list of MSDs/Genie files placed in RawData folder 
# note- the code is currently written to read zipped files, no need to extract the data)
# comment/uncomment lines depending on if you are doing MSDs or Genie extracts

# If using Genie extracts downloaded before the data is frozen
# You can use the multi-OU export, but you will need to break it up into multiple exports
# by indicator or region, or export OU-specific files
# export the following indicators from Genie after 5pm on the last day of data entry
# HTS_TST, HTS_TST_POS, LAB_PTCQI, PMTCT_ART, PMTCT_STAT, 
# TB_ART, TB_STAT, TB_PREV, TX_CURR, TX_NEW, TX_TB, VMMC_CIRC
#filelist <- list.files(RawDataPath,pattern="SITE_IM")
filelist <- list.files(RawDataPath,pattern="SITE_IM")

#if MSDs downloaded from Pano
#filelist <- list.files(RawDataPath,pattern="Site_IM")

datalist = list()   #Create empty object to store each OU output dataset
lablist = list()    #Create empty object to store the lab data for each OU
duplist = list()    #Create empty object to store the site-level deduplications


  genie_zip <- filelist[1]
  
  #get zip file name
  genie_filepath <- paste0(RawDataPath,genie_zip)
  
  #get text file name from inside the zip file
  filex <- unzip(genie_filepath, list = TRUE) %>% .$Name
  
  
  #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
  # ============= Pulling in required datasets ~~~~~~~============
  #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
 # close(unz(genie_filepath,filex))

  varlist1 <- c("orgunituid", 
                "operatingunit",
                "countryname",
                "indicator", 
                "disaggregate",
                #"categoryoptioncombouid",
                "categoryoptioncomboname", 
                "numeratordenom",
                "indicatortype",
                "fundingagency",
                "mech_code")
  
  indlist <- c("HTS_TST",
              "HTS_TST_POS",
              #"LAB_PTCQI",
              "PMTCT_ART",
              "PMTCT_STAT",
              "TB_ART",
              "TB_STAT",
              "TB_PREV",
              "TX_CURR",
              "TX_NEW",
              "TX_TB",
              "VMMC_CIRC")
    print(genie_filepath)
#    datim <- read_tsv(file=unz(genie_filepath,filex), col_names=TRUE,col_types=colvec)
    datim <- read_tsv(file = unz(genie_filepath, filex), 
                      col_names = TRUE,
                      col_types = cols(.default   = col_character(), 
                                       targets    = col_double(),
                                       qtr1       = col_double(),
                                       qtr2       = col_double(),
                                       qtr3       = col_double(),
                                       qtr4       = col_double(),
                                       cumulative = col_double())) #%>%
      #filter(indicatortype != "CS")

    # ou <- unique(datim$operatingunit)
   
      dx<- datim %>% 
      filter(indicator %in% all_of(indlist) & fiscal_year %in% c("2022","2021")) %>% # TODO
      mutate(operatingunit = case_when(operatingunit %in% c("Asia Region", "Western Hemisphere Region", "West Africa Region") ~ countryname, TRUE ~ operatingunit)) %>% 
      select(varlist1, -countryname, fiscal_year,
           cumulative,targets) %>% 
      #      qtr1:qtr4,targets) %>%
      gather(period, value, cumulative,targets) %>% 
      #gather(period, value, qtr1:qtr4,targets) %>% 
        group_by_if(is.character) %>% 
        summarize(valuex = sum(value, na.rm = T)) %>% 
        ungroup() %>% 
      filter(!is.na(valuex) & valuex != 0) %>%
      #for fy20- just filter the disaggreggates we will report and use for QC
      filter(disaggregate %in% c("Total Numerator","Total Denominator"))
      
      # if you want to go back to the original disaggregates, use the code below
      #        (indicator %in% c("HTS_TST","HTS_TST_POS") & standardizeddisaggregate %in% c("Modality/Age/Sex/Result","Modality/Age Aggregated/Sex/Result")) |
      #        (indicator %in% c("TX_CURR","TX_NEW") & standardizeddisaggregate %in% c("Age/Sex/HIVStatus","Age Aggregated/Sex/HIVStatus")) |
      #        (indicator=="PMTCT_ART" & standardizeddisaggregate %in% c("Age/NewExistingArt/Sex/HIVStatus","Age/Sex/KnownNewResult")) |
      #        (indicator=="PMTCT_STAT" & standardizeddisaggregate %in% c("Age/Sex","Age/Sex/KnownNewResult")) |
      #        (indicator=="TB_ART" & standardizeddisaggregate %in% c("Age/Sex/NewExistingArt/HIVStatus","Age/Sex/KnownNewPosNeg")) |
      #        (indicator=="TB_PREV" & standardizeddisaggregate %in% c("Age/Sex/NewExistingArt/HIVStatus","Age/NewExistingArt/HIVStatus")) |
      #        (indicator=="TB_STAT" & standardizeddisaggregate %in% c("Age/Sex/KnownNewPosNeg","Age/Sex","Age Aggregated/Sex")) |
      #        (indicator=="TX_TB" & standardizeddisaggregate %in% c("Age Aggregated/Sex/NewExistingArt/HIVStatus","Age Aggregated/Sex/TBScreen/NewExistingART/HIVStatus","Age/Sex/TBScreen/NewExistingART/HIVStatus")) |
      #        (indicator=="VMMC_CIRC" & standardizeddisaggregate=="Age/Sex"))
    
    
  
     #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    # ====  Creating the Lab Indicator - check and send final output to Erin Rottinghaus-Romano    ====
    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
if(labdata==1 & nrow(filter(datim,fiscal_year=="2021" & indicator=="LAB_PTCQI" & standardizeddisaggregate %in% c("Lab/CQI","POCT/CQI")))>0){
    # TODO
    labdatim<-datim %>%
      #filter(indicator=="LAB_PTCQI" & ((fiscal_year=="2019" & cumulative>0) | (fiscal_year=="2020 & targets>0)) & 
      #filter(indicator=="LAB_PTCQI" & ((fiscal_year=="2020" & cumulative>0) | (fiscal_year=="2021" & targets>0)) & 
      filter(indicator=="LAB_PTCQI" & (fiscal_year=="2021" & cumulative>0) & # TODO
                standardizeddisaggregate %in% c("Lab/CQI","POCT/CQI")) %>% 
      mutate(operatingunit = case_when(operatingunit %in% c("Asia Region", "Western Hemisphere Region", "West Africa Region") ~ countryname, TRUE ~ operatingunit)) 
  
        #number of labs with CQI by agency
    cqi_labs_agency <- labdatim %>%
      filter(otherdisaggregate!="Testing with no participation") %>%
      distinct(operatingunit,orgunituid,fiscal_year,fundingagency) %>%
      group_by(operatingunit,fiscal_year,fundingagency) %>%
      summarize(lab_facilities=n()) %>%
      mutate(type="CQI") %>%
      ungroup()
    
    
    #number of labs by agency
    all_labs_agency <- labdatim %>%
      distinct(operatingunit,orgunituid,fiscal_year,fundingagency) %>%
      group_by(operatingunit,fiscal_year,fundingagency) %>%
      summarize(lab_facilities=n()) %>%
      mutate(type="Total") %>%
      ungroup()

    #number of labs with CQI overall
    cqi_labs_pepfar <- labdatim %>%
      filter(otherdisaggregate!="Testing with no participation") %>%
      distinct(operatingunit,fiscal_year,orgunituid) %>%
      group_by(operatingunit,fiscal_year) %>%
      summarize(lab_facilities=n()) %>%
      mutate(type="CQI",fundingagency="PEPFAR") %>%
      ungroup()

    #number of labs overall
    all_labs_pepfar<-labdatim %>%
      distinct(operatingunit,fiscal_year,orgunituid) %>%
      group_by(operatingunit, fiscal_year) %>%
      summarize(lab_facilities=n()) %>%
      mutate(type="Total",fundingagency="PEPFAR") %>%
      ungroup()
    
    
    
    all_lab<-rbind(cqi_labs_agency,all_labs_agency,cqi_labs_pepfar,all_labs_pepfar) %>%
      spread(type,lab_facilities,fill=0) 
    
    lab_dummy<-all_labs_pepfar[FALSE,FALSE] %>%
      mutate(operatingunit=NA_character_,
             fiscal_year=NA_character_,
             fundingagency=NA_character_,
             CQI=NA,
             Total=NA)
    
    final_lab<-bind_rows(all_lab,lab_dummy) %>%
      select(operatingunit,fiscal_year,fundingagency,CQI,Total)

   # lablist[[i]] <- final_lab # add it to your list
    lablist[[1]] <- final_lab # add it to your list
    
    rm(labdatim, cqi_labs_agency,all_labs_agency,cqi_labs_pepfar,all_labs_pepfar,all_lab,final_lab)
}
    
if(labdata==1 & nrow(filter(datim,fiscal_year=="2021" & indicator=="LAB_PTCQI"))==0){ # TODO
  #print(paste(ou, "has no LAB_PTCQI indicator data for FY20, confirm"))
}
  # need to close the connection from when we created the datim data set
  close(unz(genie_filepath,filex))
  
  #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
  # ====Creating datasets for each data point used for estimation of dedup type=====
  #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
  # 
  # Separate datasets are required as the grouping variables and summing variables are non exclusive.  
  # ====The non-exclusive group sets are as follows: ========
  # (1)
  # dedup_dsd
  # dedup_ta
  # xdedup
  # 
  # (2)
  # max_dsd
  # max_ta
  # 
  # (3)
  # sum_dsd_all
  # sum_ta_all
  # 
  # (4)
  # num_cdc_im_dsd
  # num_ag_im_dsd
  # num_cdc_im_ta
  # num_ag_im_ta
  # 
  # (5)
  # max_cdc_dsd
  # max_ag_dsd
  # max_cdc_ta
  # max_ag_ta
  # 
  # (6)
  # sum_cdc_dsd
  # sum_ag_dsd
  # sum_cdc_ta
  # sum_ag_ta
  
  
  
  #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
  # ==== running the data summarizing steps =====
  #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
  ## Group-set 1
  # dedup_dsd
  # dedup_ta
  # xdedup
  
  varlist2 <- c("period",
                "fiscal_year",
                "orgunituid",
                "operatingunit",
                "indicator",
                "disaggregate",
                #"categoryoptioncombouid",
                "categoryoptioncomboname", 
                "numeratordenom",
                "colvar", 
                "valuex")
  
  g1 <- dx %>% 
    mutate(colvar = case_when(
      mech_code %in% ("00000") & indicatortype %in% c("DSD")      ~ "dedup_dsd",
      mech_code %in% ("00000") & indicatortype %in% c("TA")       ~ "dedup_ta",
      mech_code %in% ("00001") & indicatortype %in% c("TA")       ~ "xdedup",
      mech_code %in% ("00000") & indicatortype %ni% c("DSD","TA") ~ "dedup_na"
    )) %>% 
    filter(!is.na(colvar)) %>%
    select(all_of(varlist2)) %>% 
    group_by_if(is.character) %>% 
    summarize(valu = sum(valuex, na.rm=T)) %>%
    ungroup() %>% 
    filter(!is.na(valu) & valu != 0) %>% 
    mutate(valu = abs(valu))
  
  
  # # Group-set 2
  # max_dsd
  # max_ta
  
  g2 <- dx %>% 
    mutate(colvar = case_when(
      mech_code %ni% c("00000", "00001") & indicatortype %in% c("DSD")            ~ "max_dsd",
      mech_code %ni% c("00000", "00001") & indicatortype %ni% c("DSD","TA")       ~ "max_na",
      mech_code %ni% c("00000", "00001") & indicatortype %in% c("TA")             ~ "max_ta")) %>% 
    filter(!is.na(colvar)) %>% 
    select(all_of(varlist2)) %>% 
    group_by_if(is.character) %>% 
    summarize(valu = max(valuex, na.rm=T)) %>%
    ungroup() %>% 
    filter(!is.na(valu) & valu != 0) %>% 
    mutate(valu = abs(valu))
  
  
  
  # # Group-set 3
  # sum_dsd_all
  # sum_ta_all
  # sum_na_all
  
  g3 <- dx %>% 
    mutate(colvar = case_when(
      mech_code %ni% c("00001") & indicatortype %in% c("DSD")            ~ "sum_dsd_all",
      mech_code %ni% c("00001") & indicatortype %ni% c("DSD","TA")       ~ "sum_na_all",
      mech_code %ni% c("00001") & indicatortype %in% c("TA")             ~ "sum_ta_all")) %>% 
    filter(!is.na(colvar)) %>% 
    select(all_of(varlist2)) %>% 
    group_by_if(is.character) %>% 
    summarize(valu = sum(valuex, na.rm=T)) %>%
    ungroup() %>% 
    filter(!is.na(valu) & valu != 0) %>% 
    mutate(valu = abs(valu))
  
  
  
  # # Group-set 4
  # num_cdc_im_dsd
  # num_ag_im_dsd
  # num_cdc_im_ta
  # num_ag_im_ta
  # num_cdc_im_na
  # num_ag_im_na
  
  g4 <- dx %>% 
    mutate(colvar = case_when(
      mech_code %ni% c("00000","00001") & fundingagency=="HHS/CDC" & indicatortype=="DSD" ~ "num_cdc_im_dsd",
      mech_code %ni% c("00000","00001") & fundingagency=="HHS/CDC" & indicatortype=="TA"  ~ "num_cdc_im_ta",
      mech_code %ni% c("00000","00001") & fundingagency=="HHS/CDC" & 
        indicatortype %ni% c("DSD","TA")                                     ~ "num_cdc_im_na",
      mech_code %ni% c("00000","00001") & fundingagency!="HHS/CDC" & indicatortype=="DSD" ~ "num_ag_im_dsd",
      mech_code %ni% c("00000","00001") & fundingagency!="HHS/CDC" & indicatortype=="TA"  ~ "num_ag_im_ta",
      mech_code %ni% c("00000","00001") & fundingagency!="HHS/CDC" & 
        indicatortype %ni% c("DSD","TA")                                     ~ "num_ag_im_na")) %>% 
    filter(!is.na(colvar)) %>% 
    select(all_of(varlist2)) %>% 
    group_by_if(is.character) %>% 
    summarize(valu = n()) %>%
    ungroup() %>% 
    filter(!is.na(valu) & valu != 0) %>% 
    mutate(valu = abs(valu))
  
  
  # Group-set (5)
  # max_cdc_dsd
  # max_ag_dsd
  # max_cdc_ta
  # max_ag_ta
  # max_cdc_na
  # max_ag_na
  
  g5 <- dx %>% 
    mutate(colvar = case_when(
      mech_code %ni% c("00000","00001") & fundingagency=="HHS/CDC" & indicatortype=="DSD" ~ "max_cdc_dsd",
      mech_code %ni% c("00000","00001") & fundingagency=="HHS/CDC" & indicatortype=="TA"  ~ "max_cdc_ta",
      mech_code %ni% c("00000","00001") & fundingagency!="HHS/CDC" & indicatortype=="DSD" ~ "max_ag_dsd",
      mech_code %ni% c("00000","00001") & fundingagency!="HHS/CDC" & indicatortype=="TA"  ~ "max_ag_ta",
      mech_code %ni% c("00000","00001") & fundingagency=="HHS/CDC" & 
        indicatortype %ni% c("DSD","TA")                                     ~ "max_cdc_na",
      mech_code %ni% c("00000","00001") & fundingagency!="HHS/CDC" & 
        indicatortype %ni% c("DSD","TA")                                     ~ "max_ag_na")) %>% 
    filter(!is.na(colvar)) %>% 
    select(all_of(varlist2)) %>% 
    group_by_if(is.character) %>% 
    summarize(valu = max(valuex, na.rm=T)) %>%
    ungroup() %>% 
    filter(!is.na(valu) & valu != 0) %>% 
    mutate(valu = abs(valu))
  
  
  
  # Group-set (6)
  # sum_cdc_dsd
  # sum_ag_dsd
  # sum_cdc_ta
  # sum_ag_ta
  # sum_cdc_na
  # sum_ag_na
  
  g6 <- dx %>% 
    mutate(colvar = case_when(
      mech_code %ni% c("00000","00001") & fundingagency=="HHS/CDC" & indicatortype=="DSD" ~ "sum_cdc_dsd",
      mech_code %ni% c("00000","00001") & fundingagency=="HHS/CDC" & indicatortype=="TA"  ~ "sum_cdc_ta",
      mech_code %ni% c("00000","00001") & fundingagency=="HHS/CDC" &
        indicatortype %ni% c("DSD","TA")  ~ "sum_cdc_na",
      mech_code %ni% c("00000","00001") & fundingagency!="HHS/CDC" & indicatortype=="DSD" ~ "sum_ag_dsd",
      mech_code %ni% c("00000","00001") & fundingagency!="HHS/CDC" & indicatortype=="TA"  ~ "sum_ag_ta",
      mech_code %ni% c("00000","00001") & fundingagency!="HHS/CDC" & 
        indicatortype %ni% c("DSD","TA")  ~ "sum_ag_na")) %>% 
    filter(!is.na(colvar)) %>% 
    select(all_of(varlist2)) %>% 
    group_by_if(is.character) %>% 
    summarize(valu = sum(valuex, na.rm=T)) %>%
    ungroup() %>% 
    filter(!is.na(valu) & valu != 0) %>% 
    mutate(valu = abs(valu))
  
  #rm(dx)
  
  # Merging all the data in long format and then to go wide
  gfinal <- bind_rows(g1,g2,g3,g4,g5,g6) %>% 
    spread(colvar, valu) %>% 
    mutate_if(is.numeric, ~replace(., is.na(.), 0))
  
  #rm(g1,g2,g3,g4,g5)
  
  # Making sure the column variables are there
  dummyx <- gfinal[FALSE, FALSE] %>% 
    mutate(dedup_dsd     = 0,
           xdedup        = 0,
           dedup_ta      = 0,
           dedup_na      = 0,
           max_dsd       = 0,
           max_ta        = 0,
           max_na        = 0,
           sum_dsd_all   = 0,
           sum_ta_all    = 0,
           sum_na_all    = 0,
           num_ag_im_dsd = 0,
           num_cdc_im_dsd= 0,
           num_cdc_im_ta = 0,
           num_ag_im_ta  = 0,
           num_cdc_im_na = 0,
           num_ag_im_na  = 0,
           max_ag_dsd    = 0,
           max_cdc_dsd   = 0,
           max_cdc_ta    = 0,
           max_ag_ta     = 0,
           max_cdc_na    = 0,
           max_ag_na     = 0,
           sum_ag_dsd    = 0,
           sum_cdc_dsd   = 0,
           sum_cdc_ta    = 0,
           sum_ag_ta     = 0,
           sum_cdc_na    = 0,
           sum_ag_na     = 0)
  
  
  
  #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
  # ==== Identifying the dedup type: Sum, Max, or Custom =====
  #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
  # Joining dummy and wide datasets 
  # AND then identify  type of Dedup and XDedup 
  # (Sum, Max or Custom)
  dx1 <- bind_rows(gfinal, dummyx) %>% 
    mutate(dedup_type_dsd = 
             if_else(dedup_dsd==0,                          "S",
                     if_else(max_dsd == sum_dsd_all,            "M","C")),
           dedup_type_ta = 
             if_else(dedup_ta==0,                           "S",
                     if_else(max_ta == sum_ta_all,              "M","C")),
           dedup_type_na = 
             if_else(dedup_na==0,                          "S",
                     if_else(max_na == sum_na_all,            "M","C")),
           xdedup_type =
             if_else(xdedup==0,                             "S",
                     if_else(abs(xdedup) == pmin(sum_dsd_all,sum_ta_all), "M","C"))) %>% 
    mutate_if(is.numeric, ~replace(., is.na(.), 0))  
  
  #rm(gfinal, dummyx)
  
  #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
  # ====  Creating the Estimates (Algorithm core!)  =====
  #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
  fdup <- dx1 %>% 
    # Getting the point estimate for DSD
    mutate(fdup_dsd =
             case_when(num_cdc_im_dsd > 1  ~
                         case_when(dedup_type_dsd %in% c("M")  ~
                                     if_else(# When only CDC IMs at site for DSD (scenario~1.1)
                                       num_ag_im_dsd==0, abs(dedup_dsd),
                                       # When CDC is max and non-CDC IMs (scenario~1.2)
                                       if_else(max_cdc_dsd >= max_ag_dsd, sum_cdc_dsd - max_cdc_dsd,
                                               0)),
                                   dedup_type_dsd %in% c("C") 
                                   & num_ag_im_dsd==0          ~ abs(dedup_dsd),
                                   TRUE ~ 0),
                       TRUE ~ 0)) %>% 
    # Getting the point estimate for NA
    mutate(fdup_na =
             case_when(num_cdc_im_na > 1  ~
                         case_when(dedup_type_na %in% c("M")  ~
                                     if_else(# When only CDC IMs at site for na (scenario~1.1)
                                       num_ag_im_na==0, abs(dedup_na),
                                       # When CDC is max and non-CDC IMs (scenario~1.2)
                                       if_else(max_cdc_na >= max_ag_na, sum_cdc_na - max_cdc_na,
                                               0)),
                                   dedup_type_na %in% c("C") 
                                   & num_ag_im_na==0          ~ abs(dedup_na),
                                   TRUE ~ 0),
                       TRUE ~ 0)) %>% 
    # Getting the point estimate for TA
    mutate(fdup_ta =
             case_when(num_cdc_im_ta > 1  ~
                         case_when(dedup_type_ta %in% c("M")  ~
                                     if_else(# When only CDC IMs at site for DSD (scenario~1.1)
                                       num_ag_im_ta==0, abs(dedup_ta),
                                       # When CDC is max and non-CDC IMs (scenario~1.2)
                                       if_else(max_cdc_ta >= max_ag_ta, sum_cdc_ta - max_cdc_ta,
                                               0)),
                                   dedup_type_ta %in% c("C") 
                                   & num_ag_im_ta==0          ~ abs(dedup_ta),
                                   TRUE ~ 0),
                       TRUE ~ 0)) %>%  
    # Getting the low estimate for DSD
    mutate(fdup_dsd_low =
             case_when(num_cdc_im_dsd > 1  ~
                         case_when(dedup_type_dsd %in% c("M") # CDC is not max
                                   & max_cdc_dsd < max_ag_dsd   ~ pmax(0,(sum_cdc_dsd - max_ag_dsd)),
                                   dedup_type_dsd %in% c("C") 
                                   & num_ag_im_dsd > 0          ~ 
                                     case_when(
                                       num_ag_im_dsd == 1 &  #  only 1 non-CDC IM and CDC is max
                                         max_cdc_dsd >= max_ag_dsd   ~ pmax(0, (abs(dedup_dsd)-max_ag_dsd)),
                                       num_ag_im_dsd == 1 &  # only 1 non-CDC IM CDC is not max 
                                         max_cdc_dsd < max_ag_dsd   ~ 
                                         pmax(0, (abs(dedup_dsd)-pmin(sum_cdc_dsd,max_ag_dsd))), 
                                       TRUE ~ 0
                                     ), TRUE ~ 0), TRUE ~ 0)) %>% 
    # Getting the low estimate for NA
    mutate(fdup_na_low =
             case_when(num_cdc_im_na > 1  ~
                         case_when(dedup_type_na %in% c("M") # CDC is not max
                                   & max_cdc_na < max_ag_na   ~ pmax(0,(sum_cdc_na - max_ag_na)),
                                   dedup_type_na %in% c("C") 
                                   & num_ag_im_na > 0          ~ 
                                     case_when(
                                       num_ag_im_na == 1 &  #  only 1 non-CDC IM and CDC is max
                                         max_cdc_na >= max_ag_na   ~ pmax(0, (abs(dedup_na)-max_ag_na)),
                                       num_ag_im_na == 1 &  # only 1 non-CDC IM CDC is not max 
                                         max_cdc_na < max_ag_na   ~ 
                                         pmax(0, (abs(dedup_na)- pmin(sum_cdc_na,max_ag_na))), 
                                       TRUE ~ 0
                                     ), TRUE ~ 0), TRUE ~ 0)) %>% 
    # Getting the low estimate for TA
    mutate(fdup_ta_low =
             case_when(num_cdc_im_ta > 1  ~
                         case_when(dedup_type_ta %in% c("M") # CDC is not max
                                   & max_cdc_ta < max_ag_ta   ~ pmax(0,(sum_cdc_ta - max_ag_ta)),
                                   dedup_type_ta %in% c("C") 
                                   & num_ag_im_ta > 0          ~ 
                                     case_when(
                                       num_ag_im_ta == 1 &  #  only 1 non-CDC IM and CDC is max
                                         max_cdc_ta >= max_ag_ta   ~ pmax(0, (abs(dedup_ta)-max_ag_ta)),
                                       num_ag_im_ta == 1 &  # only 1 non-CDC IM CDC is not max 
                                         max_cdc_ta < max_ag_ta   ~ 
                                         pmax(0, (abs(dedup_ta)- pmin(sum_cdc_ta,max_ag_ta))),
                                       TRUE ~ 0
                                     ), TRUE ~ 0), TRUE ~ 0)) %>% 
    # Getting the high estimate for DSD
    mutate(fdup_dsd_high =
             case_when(num_cdc_im_dsd > 1  ~
                         case_when(dedup_type_dsd %in% c("M") # CDC is not max
                                   & max_cdc_dsd < max_ag_dsd   ~ sum_cdc_dsd - max_cdc_dsd,
                                   dedup_type_dsd %in% c("C") 
                                   & num_ag_im_dsd > 0          ~ pmin(abs(dedup_dsd), (sum_cdc_dsd - max_cdc_dsd)),
                                   TRUE ~ 0
                         ), TRUE ~ 0)) %>% 
    # Getting the high estimate for NA
    mutate(fdup_na_high =
             case_when(num_cdc_im_na > 1  ~
                         case_when(dedup_type_na %in% c("M") # CDC is not max
                                   & max_cdc_na < max_ag_na   ~ sum_cdc_na - max_cdc_na,
                                   dedup_type_na %in% c("C") 
                                   & num_ag_im_na > 0          ~ pmin(abs(dedup_na), (sum_cdc_na - max_cdc_na)),
                                   TRUE ~ 0
                         ), TRUE ~ 0)) %>% 
    # Getting the high estimate for TA
    mutate(fdup_ta_high =
             case_when(num_cdc_im_ta > 1  ~
                         case_when(dedup_type_ta %in% c("M") # CDC is not max
                                   & max_cdc_ta < max_ag_ta   ~ sum_cdc_ta - max_cdc_ta,
                                   dedup_type_ta %in% c("C") 
                                   & num_ag_im_ta > 0          ~ pmin(abs(dedup_ta), (sum_cdc_ta - max_cdc_ta)),
                                   TRUE ~ 0
                         ), TRUE ~ 0)) %>% 
    ## FLAGS to indicate whether the estimate is a point estimate or range, or murky (unestimable)
    mutate(fdup_dsd_type = 
             case_when(fdup_dsd > 0   ~ "Point",
                       fdup_dsd_high > 0 | fdup_dsd_low > 0 ~ "Range",
                       num_cdc_im_dsd > 1 & dedup_type_dsd %in% c("C") & num_ag_im_dsd > 1 ~ "Unestimable",
                       fdup_dsd > 0 & (fdup_dsd_high > 0 | fdup_dsd_low) ~ "XX_wrong_XX"
             )) %>% 
    mutate(fdup_na_type = 
             case_when(fdup_na > 0   ~ "Point",
                       fdup_na_high > 0 | fdup_na_low > 0 ~ "Range",
                       num_cdc_im_na > 1 & dedup_type_na %in% c("C") & num_ag_im_na > 1 ~ "Unestimable",
                       fdup_na > 0 & (fdup_na_high > 0 | fdup_na_low) ~ "XX_wrong_XX"
             )) %>% 
    mutate(fdup_ta_type = 
             case_when(fdup_ta > 0   ~ "Point",
                       fdup_ta_high > 0 | fdup_ta_low > 0 ~ "Range",
                       num_cdc_im_ta > 1 & dedup_type_ta %in% c("C") & num_ag_im_ta > 1 ~ "Unestimable",
                       fdup_ta > 0 & (fdup_ta_high > 0 | fdup_ta_low) ~ "XX_wrong_XX"
             )) %>% 
    mutate_if(is.numeric, ~replace(., is.na(.), 0))  
  
  rm(dx1)
  
  #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
  # ====  Creating the Estimates for Cross-walk Deduplication ====
  #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
  
  xfdup <- fdup %>% 
    ## Creating the overall CDC values to calculate Crosswalk dedup values
    mutate(xdup_on = if_else(xdedup_type %in% c("M","C") & 
                               sum_cdc_dsd > 0 & 
                               sum_cdc_ta >0, 1, 0),
           sum_cdc_dsd_f = case_when(xdup_on == 1   ~
                                       case_when(fdup_dsd_type %in% c("Point") ~ sum_cdc_dsd-fdup_dsd,
                                                 TRUE ~ 0),
                                     TRUE ~ 0),
           sum_cdc_ta_f = case_when(xdup_on == 1   ~
                                      case_when(fdup_ta_type %in% c("Point") ~ sum_cdc_ta-fdup_ta,
                                                TRUE ~ 0),
                                    TRUE ~ 0),
           sum_cdc_dsd_f_low = case_when(xdup_on == 1   ~
                                           case_when(fdup_dsd_type %in% c("Range") ~ sum_cdc_dsd-fdup_dsd_high,
                                                     TRUE ~ 0),
                                         TRUE ~ 0),
           sum_cdc_ta_f_low = case_when(xdup_on == 1   ~
                                          case_when(fdup_ta_type %in% c("Range") ~ sum_cdc_ta-fdup_ta_high,
                                                    TRUE ~ 0),
                                        TRUE ~ 0),
           sum_cdc_dsd_f_high = case_when(xdup_on == 1   ~
                                            case_when(fdup_dsd_type %in% c("Range") ~ sum_cdc_dsd-fdup_dsd_low,
                                                      TRUE ~ 0),
                                          TRUE ~ 0),
           sum_cdc_ta_f_high = case_when(xdup_on == 1   ~
                                           case_when(fdup_ta_type %in% c("Range") ~ sum_cdc_ta-fdup_ta_low,
                                                     TRUE ~ 0),
                                         TRUE ~ 0),
           p_xdedup = 	case_when(xdedup_type %in% c("C") &
                                   sum_cdc_dsd > 0 & 
                                   sum_cdc_ta >0   ~ abs(xdedup)/(pmin(sum_dsd_all, sum_ta_all)))
    ) %>% 
    ## Creating estimates for the Xdedup values 
    mutate(xfdup = case_when(xdup_on == 1 ~
                               case_when(
                                 # When only CDC IMs at site for both DSD and TA, same for MAX or CUSTOM
                                 num_ag_im_dsd==0 & num_ag_im_ta==0 ~ abs(xdedup),
                                 fdup_dsd_type %in% c("Point") & fdup_ta_type %in% c("Point") ~ 
                                   case_when(xdedup_type %in% c("M") ~
                                               pmin( abs(xdedup), pmin(sum_cdc_dsd_f, sum_cdc_ta_f) ),
                                             xdedup_type %in% c("C") ~
                                               pmin( abs(xdedup), p_xdedup*(pmin(sum_cdc_dsd_f, sum_cdc_ta_f))),
                                             TRUE ~ 0
                                   )), TRUE ~ 0)) %>% 
    mutate(xfdup_high = case_when(xdup_on == 1 ~
                                    case_when(xdedup_type %in% c("M")  ~
                                                case_when(
                                                  fdup_dsd_type %in% c("Point") & fdup_ta_type %in% c("Range") ~
                                                    pmin( abs(xdedup), pmin(sum_cdc_dsd_f, sum_cdc_ta_f_low) ),
                                                  fdup_dsd_type %in% c("Range") & fdup_ta_type %in% c("Point") ~
                                                    pmin(abs(xdedup), pmin(sum_cdc_dsd_f_low, sum_cdc_ta_f) ),
                                                  fdup_dsd_type %in% c("Range") & fdup_ta_type %in% c("Range") ~
                                                    pmin( abs(xdedup), pmin(sum_cdc_dsd_f_low, sum_cdc_ta_f_low) ),
                                                  TRUE ~ 0),
                                              xdedup_type %in% c("C")  ~
                                                case_when(
                                                  fdup_dsd_type %in% c("Point") & fdup_ta_type %in% c("Range") ~
                                                    pmin( abs(xdedup), p_xdedup*pmin(sum_cdc_dsd_f, sum_cdc_ta_f_low) ),
                                                  fdup_dsd_type %in% c("Range") & fdup_ta_type %in% c("Point") ~
                                                    pmin( abs(xdedup), p_xdedup*pmin(sum_cdc_dsd_f_low, sum_cdc_ta_f) ),
                                                  fdup_dsd_type %in% c("Range") & fdup_ta_type %in% c("Range") ~
                                                    pmin( abs(xdedup), p_xdedup*pmin(sum_cdc_dsd_f_low, sum_cdc_ta_f_low) ),
                                                  TRUE ~ 0), 
                                              TRUE ~ 0), TRUE ~ 0)) %>% 
    mutate(xfdup_low = case_when(xdup_on == 1 ~
                                   case_when(xdedup_type %in% c("M")  ~
                                               case_when(
                                                 fdup_dsd_type %in% c("Point") & fdup_ta_type %in% c("Range") ~
                                                   pmin( abs(xdedup), pmin(sum_cdc_dsd_f, sum_cdc_ta_f_high) ),
                                                 fdup_dsd_type %in% c("Range") & fdup_ta_type %in% c("Point") ~
                                                   pmin( abs(xdedup), pmin(sum_cdc_dsd_f_high, sum_cdc_ta_f) ),
                                                 fdup_dsd_type %in% c("Range") & fdup_ta_type %in% c("Range") ~
                                                   pmin( abs(xdedup), pmin(sum_cdc_dsd_f_high, sum_cdc_ta_f_high) ),
                                                 TRUE ~ 0),
                                             xdedup_type %in% c("C")  ~
                                               case_when(
                                                 fdup_dsd_type %in% c("Point") & fdup_ta_type %in% c("Range") ~
                                                   pmin( abs(xdedup), p_xdedup*pmin(sum_cdc_dsd_f, sum_cdc_ta_f_high) ),
                                                 fdup_dsd_type %in% c("Range") & fdup_ta_type %in% c("Point") ~
                                                   pmin( abs(xdedup), p_xdedup*pmin(sum_cdc_dsd_f_high, sum_cdc_ta_f) ),
                                                 fdup_dsd_type %in% c("Range") & fdup_ta_type %in% c("Range") ~
                                                   pmin( abs(xdedup), p_xdedup*pmin(sum_cdc_dsd_f_high, sum_cdc_ta_f_high) ),
                                                 TRUE ~ 0), 
                                             TRUE ~ 0), TRUE ~ 0)) %>% 
    mutate_if(is.numeric, ~replace(., is.na(.), 0))  
  
  #rm(fdup)
  
  #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
  # ====  Creating Site-Level Data with dedups for OUs with dedup values that are different from DATIM        ====
  #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
  #output site-level dedups for OUs to refer to when checking DFPM data
  site_level_dedup<- xfdup %>%
    filter(fdup_dsd>0 | fdup_dsd_low>0 | fdup_dsd_high>0 | fdup_ta>0 | fdup_ta_low>0 | fdup_ta_high>0 | fdup_na>0 | fdup_na_low>0 | fdup_na_high>0 | xfdup>0 | xfdup_high>0 | xfdup_low>0) %>%
    mutate(fduped_dsd_max = sum_cdc_dsd -fdup_dsd -fdup_dsd_low,
           fduped_dsd_min = sum_cdc_dsd -fdup_dsd -fdup_dsd_high,
           fduped_ta_max = sum_cdc_ta -fdup_ta -fdup_ta_low,
           fduped_ta_min = sum_cdc_ta -fdup_ta -fdup_ta_high,
           fduped_na_max = sum_cdc_na -fdup_na -fdup_na_low,
           fduped_na_min = sum_cdc_na -fdup_na -fdup_na_high) %>%
    select(period,fiscal_year,orgunituid,operatingunit,indicator,
           disaggregate,
          # categoryoptioncombouid,
           categoryoptioncomboname,numeratordenom,
           num_cdc_im_dsd,num_ag_im_dsd,sum_cdc_dsd,fduped_dsd_max,fduped_dsd_min,
           num_cdc_im_ta,num_ag_im_ta,sum_cdc_ta,fduped_ta_max,fduped_ta_min,
           num_cdc_im_na,num_cdc_im_na,sum_cdc_na,fduped_na_max,fduped_na_min,
           xfdup,xfdup_high,xfdup_low) %>%
    mutate(periodtype = case_when(
      period %in% c("targets")  ~ "COP",
      indicator %in% c("TX_CURR","TX_TB") & period %in% c("qtr4","cumulative")       ~ "APR",
      indicator %ni% c("TX_CURR","TX_TB") & period %in% c("qtr1","qtr2","qtr3","qtr4","cumulative")~ "APR",
      TRUE ~ "X"
    )) %>% 
    filter(periodtype %ni% c("X"))
  #If at least one row of dedups, output as CSV with the site-level deduplications in case countries want to see where the deduplication is taking place
  if (nrow(site_level_dedup)>0){
    duplist[[1]]<-site_level_dedup
    }

  #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
  # ====  Creating OU-level final dataset ====
  #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
  # APR dataset
  varlist3 <- c("period",
                "fiscal_year",
                "operatingunit",
                "indicator",
                "disaggregate",
              #  "categoryoptioncombouid",
                "categoryoptioncomboname", 
                "numeratordenom",
                
                "sum_cdc_dsd",
                "fdup_dsd",
                "fdup_dsd_low",
                "fdup_dsd_high",
                
                "sum_cdc_ta",
                "fdup_ta",
                "fdup_ta_low",
                "fdup_ta_high",
                
                "sum_cdc_na",
                "fdup_na",
                "fdup_na_low",
                "fdup_na_high",
                
                "xfdup",
                "xfdup_high",
                "xfdup_low")


  df_apr <- xfdup %>% 
    select(varlist3) %>% 
    gather(colvarx, value, sum_cdc_dsd:xfdup_low) %>%  
    filter(!is.na(colvarx)) %>%
    mutate(periodtype = case_when(
      period %in% c("targets")  ~ "COP",
      indicator %in% c("TX_CURR","TX_TB") & period %in% c("qtr4","cumulative")       ~ "APR",
      indicator %ni% c("TX_CURR","TX_TB") & period %in% c("qtr1","qtr2","qtr3","qtr4","cumulative")~ "APR",
      TRUE ~ "X"
    )) %>% 
    filter(periodtype %ni% c("X")) %>% 
    group_by(fiscal_year, 
             periodtype,
             operatingunit, 
             indicator, 
             disaggregate,
            # categoryoptioncombouid,
             categoryoptioncomboname, 
             numeratordenom,
             colvarx) %>% 
    summarise(valu = sum(value, na.rm=T)) %>%
    ungroup() %>% 
    spread(colvarx, valu) %>% 
    mutate_if(is.numeric, ~replace(., is.na(.), 0))
  
  dummy2 <- df_apr[FALSE, FALSE] %>% 
    mutate(sum_cdc_dsd = 0,
           fdup_dsd = 0,
           fdup_dsd_low = 0,
           fdup_dsd_high = 0,
           fduped_dsd_max = 0,
           fduped_dsd_min = 0,
           
           sum_cdc_ta = 0,
           fdup_ta = 0,
           fdup_ta_low = 0,
           fdup_ta_high = 0,
           fduped_ta_max = 0,
           fduped_ta_min = 0,
           
           sum_cdc_na = 0,
           fdup_na = 0,
           fdup_na_low = 0,
           fdup_na_high = 0,
           fduped_na_max = 0,
           fduped_na_min = 0,
           
           xfdup = 0,
           xfdup_high = 0,
           xfdup_low = 0)
  
  
  
  
  # Final step in the algorithm
  df_final <- bind_rows(df_apr, 
                        # df_sapr, 
                        dummy2) %>% 
    rename(fractionalpart = numeratordenom,
           OU_name = operatingunit) %>% 
    mutate(fduped_dsd_max = sum_cdc_dsd -fdup_dsd -fdup_dsd_low,
           fduped_dsd_min = sum_cdc_dsd -fdup_dsd -fdup_dsd_high,
           fduped_ta_max = sum_cdc_ta -fdup_ta -fdup_ta_low,
           fduped_ta_min = sum_cdc_ta -fdup_ta -fdup_ta_high,
           fduped_na_max = sum_cdc_na -fdup_na -fdup_na_low,
           fduped_na_min = sum_cdc_na -fdup_na -fdup_na_high
    ) %>% 
    gather(colvar, values, fdup_dsd:fduped_na_min) %>% 
    filter(!is.na(values))
  
  
  # Restoring the original DATIM values for DSD, TA, and NA.
  datx <- g6 %>% 
    filter(colvar %in% c("sum_cdc_dsd", 
                         "sum_cdc_ta",
                         "sum_cdc_na")) %>%
    mutate(periodtype = case_when(
      period %in% c("targets")  ~ "COP",
      indicator %in% c("TX_CURR","TX_TB") & period %in% c("qtr4","cumulative")       ~ "APR",
      indicator %ni% c("TX_CURR","TX_TB") & period %in% c("qtr1","qtr2","qtr3","qtr4","cumulative")~ "APR",
      TRUE ~ "X"
    )) %>% 
    #mutate(fiscalyear = substr(period, 3, 6)) %>%
    filter(periodtype %ni% c("X")) %>% 
    group_by(fiscal_year, 
             periodtype,
             operatingunit, 
             indicator, 
             disaggregate,
           #  categoryoptioncombouid,
             categoryoptioncomboname, 
             numeratordenom,
             colvar) %>% 
    summarise(valu = sum(valu, na.rm=T)) %>%
    ungroup() %>% 
    spread(colvar, valu) 
  
  dummy3 <- df_apr[FALSE, FALSE] %>% 
    mutate(sum_cdc_dsd = NA,
           sum_cdc_ta = NA,
           sum_cdc_na = NA)
  
  datxx <- bind_rows(datx, dummy3) %>% 
    mutate(DATIM_NA        = case_when(periodtype %in% c("APR","COP") ~ sum_cdc_na),
           DATIM_DSD       = case_when(periodtype %in% c("APR","COP") ~ sum_cdc_dsd),
           DATIM_TA        = case_when(periodtype %in% c("APR","COP") ~ sum_cdc_ta))   %>%
    rename(fractionalpart = numeratordenom,
           OU_name = operatingunit) %>% 
    select(-sum_cdc_dsd,
           -sum_cdc_ta, 
           -sum_cdc_na ) %>% 
    gather(colvar, values,
           DATIM_NA ,
           DATIM_DSD,
           DATIM_TA) 
  
  
  df_final1 <-  bind_rows(df_final, datxx) %>%   
    spread(colvar, values) %>% 
    mutate(
      Dedup_DSD = case_when(
        fduped_dsd_max == fduped_dsd_min ~ as.character(format(fduped_dsd_max,big.mark = ",",trim=TRUE)),
        fduped_dsd_max != fduped_dsd_min ~ paste(format(fduped_dsd_min,big.mark=",",trim=TRUE), format(fduped_dsd_max,big.mark=",",trim=TRUE), sep="-")),
      Dedup_TA = case_when(
        fduped_ta_max == fduped_ta_min ~ as.character(format(fduped_ta_max,big.mark=",",trim=TRUE)),
        fduped_ta_max != fduped_ta_min ~ paste(format(fduped_ta_min,big.mark=",",trim=TRUE), format(fduped_ta_max,big.mark=",",trim=TRUE), sep="-")),
      Dedup_NA = case_when(
        fduped_na_max == fduped_na_min ~ as.character(format(fduped_na_max,big.mark=",",trim=TRUE)),
        fduped_na_max != fduped_na_min ~ paste(format(fduped_na_min,big.mark=",",trim=TRUE), format(fduped_na_max,big.mark=",",trim=TRUE), sep="-"))) %>% 
    mutate(Dedup_Total_max = fduped_dsd_max + fduped_ta_max + fduped_na_max - xfdup - xfdup_low,
           Dedup_Total_min = fduped_dsd_min + fduped_ta_min + fduped_na_min - xfdup - xfdup_high) %>% 
    mutate(Dedup_Total = case_when(
      Dedup_Total_max == Dedup_Total_min ~ as.character(format(Dedup_Total_max,big.mark=",",trim=TRUE)),
      Dedup_Total_max != Dedup_Total_min ~ as.character(paste(format(Dedup_Total_min,big.mark=",",trim=TRUE), format(Dedup_Total_max,big.mark=",",trim=TRUE), sep="-"))
    )) %>%
    mutate(Xfdup_Total_max = sum_cdc_dsd + sum_cdc_ta + sum_cdc_na - xfdup - xfdup_low,
           Xfdup_Total_min = sum_cdc_dsd + sum_cdc_ta + sum_cdc_na - xfdup - xfdup_high) %>% 
    mutate(Xfdup_Total = case_when(
      Xfdup_Total_max == Xfdup_Total_min ~ as.character(format(Xfdup_Total_max,big.mark=",",trim=TRUE)),
      Xfdup_Total_max != Xfdup_Total_min ~ as.character(paste(format(Xfdup_Total_min,big.mark = ",",trim=TRUE), format(Xfdup_Total_max,big.mark=",",trim=TRUE), sep="-"))
    ))
  
  # Creating the final dummy dataset 
  fdummy <- df_final1[F, F] %>% 
    mutate(         DATIM_DSD = NA	,
                    DATIM_TA	= NA,
                    DATIM_NA	= NA,
                    Dedup_Total	= NA_character_,
                    Dedup_DSD	= NA_character_,
                    Dedup_TA	= NA_character_,
                    Dedup_NA= NA_character_,
                    Xfdup_Total=NA_character_)
  
  
  # Recoding Dedup values to reflect nulls and zeros 
  df_finalx <- bind_rows(df_final1, fdummy) %>% 
    select(fiscal_year,
           periodtype,
           OU_name,
           indicator,
           disaggregate,
           #categoryoptioncombouid,
           categoryoptioncomboname, 
           fractionalpart,
           DATIM_DSD	,
           DATIM_TA,
           DATIM_NA,
           Dedup_Total	,
           Dedup_DSD	,
           Dedup_TA	,
           Dedup_NA,
           Xfdup_Total
    ) %>%   
    rowwise() %>%
    mutate(DATIM_Total=format(sum(as.numeric(DATIM_DSD),as.numeric(DATIM_TA),as.numeric(DATIM_NA),na.rm=TRUE),big.mark=",",trim=TRUE)) %>%
    mutate(DATIM_Total=case_when(is.na(DATIM_DSD) & is.na(DATIM_TA)~NA_character_,TRUE~as.character(DATIM_Total))) %>%
    mutate(DATIM_DSD = if_else(is.na(DATIM_DSD),NA_character_, as.character(format(DATIM_DSD,big.mark = ",",trim=TRUE))),
           DATIM_TA  = if_else(is.na(DATIM_TA),NA_character_,as.character(format(DATIM_TA,big.mark = ",",trim=TRUE))),
           DATIM_NA  = if_else(is.na(DATIM_NA),NA_character_,as.character(format(DATIM_NA,big.mark = ",",trim=TRUE)))) %>% 
   mutate(fiscal_year = if_else(periodtype %in% c("COP"), 
                                as.character(as.numeric(fiscal_year)-1), 
                                fiscal_year)) %>% 
    mutate(Dedup_DSD = case_when(
        is.na(DATIM_DSD) & Dedup_DSD=="0"  ~ NA_character_,
      TRUE  ~ Dedup_DSD)) %>% 
    mutate(Dedup_TA = case_when(
        is.na(DATIM_TA) & Dedup_TA=="0"  ~ NA_character_,
      TRUE  ~ Dedup_TA)) %>% 
    mutate(Dedup_NA = case_when(
        is.na(DATIM_NA) & Dedup_NA=="0"  ~ NA_character_,
      TRUE  ~ Dedup_NA)) %>% 
    mutate(Dedup_Total = 
             if_else(is.na(Dedup_DSD) &
                       is.na(Dedup_TA) &
                       is.na(Dedup_NA), NA_character_, Dedup_Total)) %>%
    mutate(Xfdup_Total = 
             if_else(is.na(Dedup_DSD) &
                       is.na(Dedup_TA) &
                       is.na(Dedup_NA), NA_character_, Xfdup_Total)) %>%
    rename(fiscalyear=fiscal_year)  %>% 
    filter(!is.na(DATIM_DSD) | !is.na(DATIM_TA) | !is.na(DATIM_NA) | !is.na(DATIM_Total) | 
             !is.na(Dedup_DSD) | !is.na(Dedup_TA) | !is.na(Dedup_NA) | !is.na(Dedup_Total) | !is.na(Xfdup_Total))
  
  
  #check if there is any data for the OU, and only output if there is any
  full<-nrow(df_finalx)
  
  anydata<-nrow(filter(df_finalx,is.na(Dedup_Total)))
  
  if((full>anydata)){
   datalist[[1]] <- df_finalx # add it to your list 
    
  }
  
  
  




#raw output for manipulation
rawoutput <-bind_rows(datalist)
write_csv(rawoutput, paste(OutPath,format(Sys.time(), "%Y%m%dT%H"),"_ALL_OUs_",timeperiod,"_Fdups_raw.csv", sep=""), na="")

# ============= Output for CDS Team ~~~~~~~============
# Stacking all the OU data together & removing extra variables and data
fdup_all_ous <-rawoutput %>% 
  filter(!(fiscalyear=="2019" & periodtype=="COP")) %>%
  filter(!is.na(Dedup_Total)) %>%
  mutate(fiscalyear=if_else(periodtype=="COP",as.numeric(fiscalyear)+1,as.numeric(fiscalyear)),
         resulttargets=case_when(periodtype=="COP"~"targets",periodtype=="APR"~"results"),
         indicator=paste(indicator," (",fractionalpart,")",sep=""),
         Revised_DSD="",
         Revised_TA="",
         Revised_Total="") %>%
  rename(Country=OU_name) %>%
  select(fiscalyear,resulttargets,Country,indicator,disaggregate,fractionalpart,
          DATIM_DSD,Dedup_DSD,Revised_DSD,DATIM_TA,Dedup_TA,Revised_TA,DATIM_Total,Dedup_Total,Revised_Total) 
  

#create FY22 dummy table for 3 CBJ indicators
countries<-select(fdup_all_ous,Country) %>%
  unique() %>%
  mutate(TX_CURR="",
         TB_ART="",
         VMMC_CIRC="") %>%
  pivot_longer(cols=TX_CURR:VMMC_CIRC) %>%
  select(-value) %>%
  rename(indicator=name)%>%
  mutate(resulttargets="targets",
         fiscalyear=2023,
         disaggregate="Total Numerator",
         fractionalpart="N",
         indicator=paste(indicator," (",fractionalpart,")",sep=""),
         DATIM_DSD="",
         Dedup_DSD="",
         Revised_DSD="",
         DATIM_TA="",
         Dedup_TA="",
         Revised_TA="",
         DATIM_Total="",
         Dedup_Total="",
         Revised_Total="") %>%
  select(-fractionalpart) 
  

finalout<-bind_rows(fdup_all_ous,countries) %>%
  ungroup() %>%
  mutate(rownum=dplyr::row_number(),
         filename=paste(format(Sys.time(), "%Y%m%dT%H"),"_ALL_OUs_",timeperiod,"_Fdups.csv", sep=""))

# Final export of the dataset for SharePoint *format from 2019- to compare to version below or see if we can incorporate into PowerBI directly using queries
openxlsx::write.xlsx(finalout %>% select(Title = rownum, fiscalyear, resulttargets, Country, indicator, disaggregate, DATIM_DSD, Dedup_DSD, Revised_DSD, DATIM_TA, Dedup_TA, Revised_TA, DATIM_Total, Dedup_Total, Revised_Total, filename)
            , paste0(format(Sys.time(), "%Y%m%dT%H"),"_ALL_OUs_",timeperiod,"_Fdups.xlsx"), asTable = T, overwrite = T)
#write_csv(fdup_all_ous, paste(Teams_Filepath,format(Sys.time(), "%Y%m%dT%H"),"_ALL_OUs_",timeperiod,"_Fdups.csv", sep=""), na="")


# ============= Output Separate Lab File ~~~~~~~============
if(labdata==1){
  lab_all_ous <- bind_rows(lablist)
  write_csv(lab_all_ous, paste(OutPath,"ALL_OUs_",timeperiod,"_lab_counts_fy20.csv", sep=""), na="")
}

# ============= Output Separate Site File ~~~~~~~============
site_dedup_all <- bind_rows(duplist)
write_csv(site_dedup_all, paste(OutPath, "ALL_OUs_", timeperiod,"_site_dedups.csv", sep=""), na="")

# ============= Output for Power BI analytics and dashboards ~~~~~~~============
# get numeric values for analytics, assigning a value for the deduplicate
# that would result in the maximum difference from the DATIM result if it were chose
# Also need to create "Total" values for if we just added DSD, TA - potential 
# double-counting without cross-walk deduplication of DSD and TA xfdup, so we output those to check
# 

test <- rawoutput %>%
  filter(!(fiscalyear=="2019" & periodtype=="COP")) %>%
  rowwise() %>%
  mutate(DATIM_DSD=as.numeric(str_remove_all(DATIM_DSD,",")),
         DATIM_TA=as.numeric(str_remove_all(DATIM_TA,",")),
         DATIM_NA=as.numeric(str_remove_all(DATIM_NA,",")),
         DATIM_Total=as.numeric(str_remove_all(DATIM_Total,",")))

test2 <-test %>%
  mutate(Dedup_DSD_Low=as.numeric(case_when(grepl("-",Dedup_DSD) ~ gsub("-.*$","",str_remove_all(Dedup_DSD,",")),TRUE~str_remove_all(Dedup_DSD,","))),
         Dedup_DSD_High=as.numeric(case_when(grepl("-",Dedup_DSD)~gsub(".*-","",str_remove_all(Dedup_DSD,",")),TRUE~str_remove_all(Dedup_DSD,","))),
         Dedup_TA_Low=as.numeric(case_when(grepl("-",Dedup_TA) ~ gsub("-.*$","",str_remove_all(Dedup_TA,",")),TRUE~str_remove_all(Dedup_TA,","))),
         Dedup_TA_High=as.numeric(case_when(grepl("-",Dedup_TA)~gsub(".*-","",str_remove_all(Dedup_TA,",")),TRUE~str_remove_all(Dedup_TA,","))),
         Dedup_Total_Low=as.numeric(case_when(grepl("-",Dedup_Total) ~ gsub("-.*$","",str_remove_all(Dedup_Total,",")),TRUE~str_remove_all(Dedup_Total,","))),
         Dedup_Total_High=as.numeric(case_when(grepl("-",Dedup_Total)~gsub(".*-","",str_remove_all(Dedup_Total,",")),TRUE~str_remove_all(Dedup_Total,","))),
         
        Xfdup_Total_Low=as.numeric(case_when(grepl("-",Xfdup_Total) ~ gsub("-.*$","",str_remove_all(Xfdup_Total,",")),TRUE~str_remove_all(Xfdup_Total,","))),
        Xfdup_Total_High=as.numeric(case_when(grepl("-",Xfdup_Total)~gsub(".*-","",str_remove_all(Xfdup_Total,",")),TRUE~str_remove_all(Xfdup_Total,","))),
         Max_DSD_Dedup=case_when(abs(DATIM_DSD-Dedup_DSD_Low)>abs(DATIM_DSD-Dedup_DSD_High)~Dedup_DSD_Low,
                                 abs(DATIM_DSD-Dedup_DSD_Low)<abs(DATIM_DSD-Dedup_DSD_High)~Dedup_DSD_High,
                                 TRUE~Dedup_DSD_Low),
         Max_TA_Dedup=case_when(abs(DATIM_TA-Dedup_TA_Low)>abs(DATIM_TA-Dedup_TA_High)~Dedup_TA_Low,
                                            abs(DATIM_TA-Dedup_TA_Low)<abs(DATIM_TA-Dedup_TA_High)~Dedup_TA_High,
                                            TRUE~Dedup_TA_Low),
         Max_Total_Dedup=case_when(abs(DATIM_Total-Dedup_Total_Low)>abs(DATIM_Total-Dedup_Total_High)~Dedup_Total_Low,
                                               abs(DATIM_Total-Dedup_Total_Low)<abs(DATIM_Total-Dedup_Total_High)~Dedup_Total_High,
                                               TRUE~Dedup_Total_Low),
       #  Max_Total_Xfdup=case_when(abs(DATIM_Total-Xfdup_Total_Low)>abs(DATIM_Total-Xfdup_Total_High)~Xfdup_Total_Low,
        #                                       abs(DATIM_Total-Xfdup_Total_Low)<abs(DATIM_Total-Xfdup_Total_High)~Xfdup_Total_High,
        #                                       TRUE~Xfdup_Total_Low),
        # Dedup_NA=as.numeric(Dedup_NA)
       ) %>%
            select(-Dedup_DSD,-Dedup_Total,-Dedup_TA,
                -Dedup_TA_Low,-Dedup_TA_High,
                  -Dedup_DSD_Low,-Dedup_DSD_High,-Dedup_Total_High,-Dedup_Total_Low
                #,  -Xfdup_Total,-Xfdup_Total_Low, -Xfdup_Total_High
                ) %>%
  rename(Dedup_DSD=Max_DSD_Dedup,Dedup_TA=Max_TA_Dedup,Dedup_Total=Max_Total_Dedup
         #,Xfdup_Total=Max_Total_Xfdup
         )

test2<-test2 %>%
  mutate(Xfdup_Total=NA)
# Final export of the numeric dataset for quality checking algorithm results
write_csv(test2, paste(OutPath,"ALL_OUs_",timeperiod,"_numeric_fdups.csv", sep=""), na="")





