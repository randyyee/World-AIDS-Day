## ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
## TITLE: World AIDS Day
## AUTHOR: Randy Yee (pcx5@cdc.gov)
## DESCRIPTION: Import and dedupe MSD
##   Export from DATIM (Frozen):
##    Operating Unit: Global,
##    Indicator: HTS_TST,HTS_TST_POS,LAB_PTCQI,PMTCT_ART,PMTCT_STAT,TB_ART,TB_PREV,TB_STAT,TX_CURR,TX_NEW,TX_TB,VMMC_CIRC,
##    Standardized Disaggregate: Lab/CQI,POCT/CQI,Total Denominator,Total Numerator,
##    Fiscal Year: ...2022,2021,2020,2019,2018,2017,2016,2015,
## CREATION DATE: 11/8/2021
## UPDATE: 
## ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

library(tidyverse)

# ============= I. Setup ============= 
`%ni%`     <- Negate(`%in%`) 

filefolder <- "C:/Users/pcx5/Downloads/"
filename   <- list.files(filefolder, pattern="SITE_IM")
timeperiod <- "FY21Q4_preclean"

thisyear   <- "2021"
nextyear   <- "2022"

# ============= II. Import Data ============= 
df_datim <- read_tsv(file = unz(paste0(filefolder, filename),
                                str_replace(filename, "zip", "txt")), 
                     col_names = TRUE,
                     col_types = cols(.default   = col_character(), 
                                      targets    = col_double(),
                                      qtr1       = col_double(),
                                      qtr2       = col_double(),
                                      qtr3       = col_double(),
                                      qtr4       = col_double(),
                                      cumulative = col_double()))

# ============= III. Subset Data ============= 
df <- df_datim %>% 
  filter(indicator %in% c("HTS_TST", 
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
                          "VMMC_CIRC") & 
           fiscal_year %in% c(thisyear, nextyear)) %>% 
  mutate(operatingunit = case_when(grepl("region", operatingunit, ignore.case = TRUE) ~ countryname, 
                                   TRUE                                               ~ operatingunit)) %>% 
  select(orgunituid, 
         operatingunit, 
         #countryname,
         indicator, 
         disaggregate,
         categoryoptioncomboname, 
         numeratordenom, 
         indicatortype,
         fundingagency, 
         mech_code, 
         fiscal_year,
         cumulative, 
         targets) %>% 
  gather(period, value, cumulative, targets) %>% 
  group_by_if(is.character) %>% 
  summarize(value = sum(value, na.rm = T)) %>% 
  ungroup() %>% 
  filter(!is.na(value) & value != 0) %>%
  filter(disaggregate %in% c("Total Numerator", "Total Denominator"))

# ============= IV. Deduplication: Summaries ============= 
vlist <- c("period",
           "fiscal_year",
           "orgunituid",
           "operatingunit",
           "indicator",
           "disaggregate",
           "categoryoptioncomboname", 
           "numeratordenom",
           "colvar", 
           "value")

df_dedupe <- df %>% 
  # Group 1: Summed Dedup Support Types
  mutate(colvar = case_when(
    mech_code %in% ("00000") & indicatortype %in% c("DSD")      ~ "dedup_dsd",
    mech_code %in% ("00000") & indicatortype %in% c("TA")       ~ "dedup_ta",
    mech_code %in% ("00001") & indicatortype %in% c("TA")       ~ "xdedup",
    mech_code %in% ("00000") & indicatortype %ni% c("DSD","TA") ~ "dedup_na")) %>% 
  filter(!is.na(colvar)) %>%
  select(all_of(vlist)) %>% 
  group_by_if(is.character) %>% 
  summarize(value = sum(value, na.rm=T)) %>%
  ungroup() %>% 
  filter(!is.na(value) & value != 0) %>% 
  mutate(value = abs(value)) %>%
  # Group 2: Maximum of Support Types
  bind_rows(
    df %>%
      mutate(colvar = case_when(
        mech_code %ni% c("00000","00001") & indicatortype %in% c("DSD")      ~ "max_dsd",
        mech_code %ni% c("00000","00001") & indicatortype %ni% c("DSD","TA") ~ "max_na",
        mech_code %ni% c("00000","00001") & indicatortype %in% c("TA")       ~ "max_ta")) %>% 
      filter(!is.na(colvar)) %>% 
      select(all_of(vlist)) %>% 
      group_by_if(is.character) %>% 
      summarize(value = max(value, na.rm=T)) %>%
      ungroup() %>% 
      filter(!is.na(value) & value != 0) %>% 
      mutate(value = abs(value))
  ) %>%
  # Group 3: Summed Support Types
  bind_rows(
    df %>%
      mutate(colvar = case_when(
        mech_code %ni% c("00001") & indicatortype %in% c("DSD")      ~ "sum_dsd_all",
        mech_code %ni% c("00001") & indicatortype %ni% c("DSD","TA") ~ "sum_na_all",
        mech_code %ni% c("00001") & indicatortype %in% c("TA")       ~ "sum_ta_all")) %>% 
      filter(!is.na(colvar)) %>% 
      select(all_of(vlist)) %>% 
      group_by_if(is.character) %>% 
      summarize(value = sum(value, na.rm=T)) %>%
      ungroup() %>% 
      filter(!is.na(value) & value != 0) %>% 
      mutate(value = abs(value))
  ) %>%
  # Group 4: Count of CDC/IM/Support Types
  bind_rows(
    df %>% 
      mutate(colvar = case_when(
        mech_code %ni% c("00000","00001") & fundingagency=="HHS/CDC" & indicatortype == "DSD" ~ "num_cdc_im_dsd",
        mech_code %ni% c("00000","00001") & fundingagency=="HHS/CDC" & indicatortype == "TA"  ~ "num_cdc_im_ta",
        mech_code %ni% c("00000","00001") & fundingagency=="HHS/CDC" & indicatortype %ni% c("DSD","TA") ~ "num_cdc_im_na",
        mech_code %ni% c("00000","00001") & fundingagency!="HHS/CDC" & indicatortype == "DSD" ~ "num_ag_im_dsd",
        mech_code %ni% c("00000","00001") & fundingagency!="HHS/CDC" & indicatortype == "TA"  ~ "num_ag_im_ta",
        mech_code %ni% c("00000","00001") & fundingagency!="HHS/CDC" & indicatortype %ni% c("DSD","TA") ~ "num_ag_im_na")) %>% 
      filter(!is.na(colvar)) %>% 
      select(all_of(vlist)) %>% 
      group_by_if(is.character) %>% 
      summarize(value = n()) %>%
      ungroup() %>% 
      filter(!is.na(value) & value != 0) %>% 
      mutate(value = abs(value))
  ) %>%
  # Group 5: Maximum
  bind_rows(
    df %>% 
      mutate(colvar = case_when(
        mech_code %ni% c("00000","00001") & fundingagency=="HHS/CDC" & indicatortype == "DSD" ~ "max_cdc_dsd",
        mech_code %ni% c("00000","00001") & fundingagency=="HHS/CDC" & indicatortype == "TA"  ~ "max_cdc_ta",
        mech_code %ni% c("00000","00001") & fundingagency!="HHS/CDC" & indicatortype == "DSD" ~ "max_ag_dsd",
        mech_code %ni% c("00000","00001") & fundingagency!="HHS/CDC" & indicatortype == "TA"  ~ "max_ag_ta",
        mech_code %ni% c("00000","00001") & fundingagency=="HHS/CDC" & indicatortype %ni% c("DSD", "TA") ~ "max_cdc_na",
        mech_code %ni% c("00000","00001") & fundingagency!="HHS/CDC" & indicatortype %ni% c("DSD", "TA") ~ "max_ag_na")) %>% 
      filter(!is.na(colvar)) %>% 
      select(all_of(vlist)) %>% 
      group_by_if(is.character) %>% 
      summarize(value = max(value, na.rm=T)) %>%
      ungroup() %>% 
      filter(!is.na(value) & value != 0) %>% 
      mutate(value = abs(value))
  ) %>%
  # Group 6: 
  bind_rows(
    df %>% 
      mutate(colvar = case_when(
        mech_code %ni% c("00000","00001") & fundingagency=="HHS/CDC" & indicatortype == "DSD" ~ "sum_cdc_dsd",
        mech_code %ni% c("00000","00001") & fundingagency=="HHS/CDC" & indicatortype == "TA"  ~ "sum_cdc_ta",
        mech_code %ni% c("00000","00001") & fundingagency=="HHS/CDC" & indicatortype %ni% c("DSD","TA")  ~ "sum_cdc_na",
        mech_code %ni% c("00000","00001") & fundingagency!="HHS/CDC" & indicatortype == "DSD" ~ "sum_ag_dsd",
        mech_code %ni% c("00000","00001") & fundingagency!="HHS/CDC" & indicatortype == "TA"  ~ "sum_ag_ta",
        mech_code %ni% c("00000","00001") & fundingagency!="HHS/CDC" & indicatortype %ni% c("DSD","TA")  ~ "sum_ag_na")) %>% 
      filter(!is.na(colvar)) %>% 
      select(all_of(vlist)) %>% 
      group_by_if(is.character) %>% 
      summarize(value = sum(value, na.rm=T)) %>%
      ungroup() %>% 
      filter(!is.na(value) & value != 0) %>% 
      mutate(value = abs(value))
  ) %>% 
  group_by_if(is.character) %>% 
  summarize(value = sum(value, na.rm=T)) %>%
  ungroup() %>% 
  pivot_wider(names_from = colvar, values_from = value) %>% 
  mutate_if(is.numeric, ~replace(., is.na(.), 0)) %>%
  bind_rows(
    .[FALSE, FALSE] %>% 
      mutate(dedup_dsd      = 0,
             xdedup         = 0,
             dedup_ta       = 0,
             dedup_na       = 0,
             max_dsd        = 0,
             max_ta         = 0,
             max_na         = 0,
             sum_dsd_all    = 0,
             sum_ta_all     = 0,
             sum_na_all     = 0,
             num_ag_im_dsd  = 0,
             num_cdc_im_dsd = 0,
             num_cdc_im_ta  = 0,
             num_ag_im_ta   = 0,
             num_cdc_im_na  = 0,
             num_ag_im_na   = 0,
             max_ag_dsd     = 0,
             max_cdc_dsd    = 0,
             max_cdc_ta     = 0,
             max_ag_ta      = 0,
             max_cdc_na     = 0,
             max_ag_na      = 0,
             sum_ag_dsd     = 0,
             sum_cdc_dsd    = 0,
             sum_cdc_ta     = 0,
             sum_ag_ta      = 0,
             sum_cdc_na     = 0,
             sum_ag_na      = 0)
  )%>%
  mutate(dedup_type_dsd = if_else(dedup_dsd == 0,  "S", if_else(max_dsd     == sum_dsd_all,                   "M","C")),
         dedup_type_ta  = if_else(dedup_ta  == 0,  "S", if_else(max_ta      == sum_ta_all,                    "M","C")),
         dedup_type_na  = if_else(dedup_na  == 0,  "S", if_else(max_na      == sum_na_all,                    "M","C")),
         xdedup_type    = if_else(xdedup    == 0,  "S", if_else(abs(xdedup) == pmin(sum_dsd_all, sum_ta_all), "M","C"))) %>%
  mutate_if(is.numeric, ~replace(., is.na(.), 0))

# ============= V. Estimations ============= 
df_fdup <- df_dedupe %>% 
  # Getting the point estimate for DSD
  mutate(fdup_dsd = case_when(
    num_cdc_im_dsd > 1 & dedup_type_dsd == "M" & num_ag_im_dsd == 0        ~ abs(dedup_dsd), # When only CDC IMs at site for DSD (scenario~1.1)
    num_cdc_im_dsd > 1 & dedup_type_dsd == "M" & max_cdc_dsd >= max_ag_dsd ~ sum_cdc_dsd - max_cdc_dsd, # When CDC is max and non-CDC IMs (scenario~1.2)
    num_cdc_im_dsd > 1 & dedup_type_dsd == "C" & num_ag_im_dsd == 0        ~ abs(dedup_dsd),
    TRUE ~ 0),
    # Getting the point estimate for NA
    fdup_na = case_when(
      num_cdc_im_na > 1 & dedup_type_na == "M" & num_ag_im_na == 0       ~ abs(dedup_na), # When only CDC IMs at site for na (scenario~1.1)
      num_cdc_im_na > 1 & dedup_type_na == "M" & max_cdc_na >= max_ag_na ~ sum_cdc_na - max_cdc_na, # When CDC is max and non-CDC IMs (scenario~1.2)
      num_cdc_im_na > 1 & dedup_type_na == "C" & num_ag_im_na == 0       ~ abs(dedup_na),
      TRUE ~ 0),
    # Getting the point estimate for TA
    fdup_ta = case_when(
      num_cdc_im_ta > 1 & dedup_type_ta == "M" & num_ag_im_ta == 0       ~ abs(dedup_ta), # When only CDC IMs at site for DSD (scenario~1.1)
      num_cdc_im_ta > 1 & dedup_type_ta == "M" & max_cdc_ta >= max_ag_ta ~ sum_cdc_ta - max_cdc_ta, # When CDC is max and non-CDC IMs (scenario~1.2)
      num_cdc_im_ta > 1 & dedup_type_ta == "C" & num_ag_im_ta == 0       ~ abs(dedup_ta),
      TRUE ~ 0),  
    # Getting the low estimate for DSD
    fdup_dsd_low = case_when(
      num_cdc_im_dsd > 1 & dedup_type_dsd == "M" & max_cdc_dsd < max_ag_dsd                                           ~ pmax(0, (sum_cdc_dsd - max_ag_dsd)), # CDC is not max
      num_cdc_im_dsd > 1 & dedup_type_dsd == "C" & num_ag_im_dsd > 0 & num_ag_im_dsd == 1 & max_cdc_dsd >= max_ag_dsd ~ pmax(0, (abs(dedup_dsd)-max_ag_dsd)),
      num_cdc_im_dsd > 1 & dedup_type_dsd == "C" & num_ag_im_dsd > 0 & num_ag_im_dsd == 1 & max_cdc_dsd < max_ag_dsd  ~ pmax(0, (abs(dedup_dsd)-pmin(sum_cdc_dsd,max_ag_dsd))),
      TRUE ~ 0), 
    # Getting the low estimate for NA
    fdup_na_low = case_when(
      num_cdc_im_na > 1 & dedup_type_na == "M" & max_cdc_na < max_ag_na                      ~ pmax(0, (sum_cdc_na - max_ag_na)),
      num_cdc_im_na > 1 & dedup_type_na == "C" & num_ag_im_na == 1 & max_cdc_na >= max_ag_na ~ pmax(0, (abs(dedup_na)-max_ag_na)), # only 1 non-CDC IM and CDC is max
      num_cdc_im_na > 1 & dedup_type_na == "C" & num_ag_im_na == 1 & max_cdc_na < max_ag_na  ~ pmax(0, (abs(dedup_na)- pmin(sum_cdc_na,max_ag_na))), # only 1 non-CDC IM CDC is not max 
      TRUE ~ 0),
    # Getting the low estimate for TA
    fdup_ta_low = case_when(
      num_cdc_im_ta > 1 & dedup_type_ta == "M" & max_cdc_ta < max_ag_ta                      ~ pmax(0, (sum_cdc_ta - max_ag_ta)), # CDC is not max
      num_cdc_im_ta > 1 & dedup_type_ta == "C" & num_ag_im_ta == 1 & max_cdc_ta >= max_ag_ta ~ pmax(0, (abs(dedup_ta)-max_ag_ta)), # only 1 non-CDC IM and CDC is max         
      num_cdc_im_ta > 1 & dedup_type_ta == "C" & num_ag_im_ta == 1 & max_cdc_ta < max_ag_ta  ~ pmax(0, (abs(dedup_ta)- pmin(sum_cdc_ta,max_ag_ta))), # only 1 non-CDC IM CDC is not max 
      TRUE ~ 0),
    # Getting the high estimate for DSD
    fdup_dsd_high = case_when(
      num_cdc_im_dsd > 1 & dedup_type_dsd == "M" & max_cdc_dsd < max_ag_dsd ~ sum_cdc_dsd - max_cdc_dsd, # CDC is not max
      num_cdc_im_dsd > 1 & dedup_type_dsd == "C" & num_ag_im_dsd > 0        ~ pmin(abs(dedup_dsd), (sum_cdc_dsd - max_cdc_dsd)),
      TRUE ~ 0),
    # Getting the high estimate for NA
    fdup_na_high = case_when(
      num_cdc_im_na > 1 & dedup_type_na == "M" & max_cdc_na < max_ag_na ~ sum_cdc_na - max_cdc_na, # CDC is not max
      num_cdc_im_na > 1 & dedup_type_na == "C" & num_ag_im_na > 0       ~ pmin(abs(dedup_na), (sum_cdc_na - max_cdc_na)),
      TRUE ~ 0),
    # Getting the high estimate for TA
    fdup_ta_high = case_when(
      num_cdc_im_ta > 1 & dedup_type_ta == "M" & max_cdc_ta < max_ag_ta ~ sum_cdc_ta - max_cdc_ta, # CDC is not max
      num_cdc_im_ta > 1 & dedup_type_ta == "C" & num_ag_im_ta > 0       ~ pmin(abs(dedup_ta), (sum_cdc_ta - max_cdc_ta)),
      TRUE ~ 0)) %>%
  # FLAGS to indicate whether the estimate is a point estimate or range, or murky (unestimable)
  mutate(fdup_dsd_type = case_when(
    fdup_dsd > 0                                                   ~ "Point",
    fdup_dsd_high > 0 | fdup_dsd_low > 0                           ~ "Range",
    num_cdc_im_dsd > 1 & dedup_type_dsd == "C" & num_ag_im_dsd > 1 ~ "Unestimable",
    fdup_dsd > 0 & (fdup_dsd_high > 0 | fdup_dsd_low)              ~ "XX_wrong_XX")) %>% 
  mutate(fdup_na_type = case_when(
    fdup_na > 0                                                    ~ "Point",
    fdup_na_high > 0 | fdup_na_low > 0                             ~ "Range",
    num_cdc_im_na > 1 & dedup_type_na == "C" & num_ag_im_na > 1    ~ "Unestimable",
    fdup_na > 0 & (fdup_na_high > 0 | fdup_na_low)                 ~ "XX_wrong_XX")) %>% 
  mutate(fdup_ta_type = case_when(
    fdup_ta > 0                                                    ~ "Point",
    fdup_ta_high > 0 | fdup_ta_low > 0                             ~ "Range",
    num_cdc_im_ta > 1 & dedup_type_ta == "C" & num_ag_im_ta > 1    ~ "Unestimable",
    fdup_ta > 0 & (fdup_ta_high > 0 | fdup_ta_low)                 ~ "XX_wrong_XX")) %>% 
  mutate_if(is.numeric, ~replace(., is.na(.), 0))  

# ============= VI. Creating the Estimates for Cross-walk Deduplication ============= 
df_xfdup <- df_fdup %>% 
  # Creating the overall CDC values to calculate Crosswalk dedup values
  mutate(xdup_on            = if_else(xdedup_type %in% c("M","C") & sum_cdc_dsd > 0 & sum_cdc_ta > 0, 1, 0),
         sum_cdc_dsd_f      = if_else(xdup_on == 1 & fdup_dsd_type == "Point", sum_cdc_dsd - fdup_dsd, 0),
         sum_cdc_ta_f       = if_else(xdup_on == 1 & fdup_ta_type  == "Point", sum_cdc_ta  - fdup_ta, 0),
         sum_cdc_dsd_f_low  = if_else(xdup_on == 1 & fdup_dsd_type == "Range", sum_cdc_dsd - fdup_dsd_high, 0),
         sum_cdc_ta_f_low   = if_else(xdup_on == 1 & fdup_ta_type  == "Range", sum_cdc_ta  - fdup_ta_high, 0),
         sum_cdc_dsd_f_high = if_else(xdup_on == 1 & fdup_dsd_type == "Range", sum_cdc_dsd - fdup_dsd_low, 0),
         sum_cdc_ta_f_high  = if_else(xdup_on == 1 & fdup_ta_type  == "Range", sum_cdc_ta  - fdup_ta_low, 0),
         p_xdedup           = case_when(xdedup_type == "C" & sum_cdc_dsd > 0 & sum_cdc_ta > 0 ~ abs(xdedup)/(pmin(sum_dsd_all, sum_ta_all)))) %>% 
  # Creating estimates for the Xdedup values 
  mutate(xfdup = case_when(
    xdup_on == 1 & num_ag_im_dsd == 0 & num_ag_im_ta == 0 ~ abs(xdedup), # When only CDC IMs at site for both DSD and TA, same for MAX or CUSTOM
    xdup_on == 1 & fdup_dsd_type =="Point" & fdup_ta_type == "Point" & xdedup_type == "M" ~ pmin(abs(xdedup), pmin(sum_cdc_dsd_f, sum_cdc_ta_f)),
    xdup_on == 1 & fdup_dsd_type =="Point" & fdup_ta_type == "Point" & xdedup_type == "C" ~ pmin(abs(xdedup), p_xdedup*(pmin(sum_cdc_dsd_f, sum_cdc_ta_f))),
    TRUE ~ 0),
    xfdup_high = case_when(
      xdup_on == 1 & xdedup_type == "M" & fdup_dsd_type == "Point" & fdup_ta_type == "Range" ~ pmin(abs(xdedup), pmin(sum_cdc_dsd_f, sum_cdc_ta_f_low)),
      xdup_on == 1 & xdedup_type == "M" & fdup_dsd_type == "Range" & fdup_ta_type == "Point" ~ pmin(abs(xdedup), pmin(sum_cdc_dsd_f_low, sum_cdc_ta_f)),
      xdup_on == 1 & xdedup_type == "M" & fdup_dsd_type == "Range" & fdup_ta_type == "Range" ~ pmin(abs(xdedup), pmin(sum_cdc_dsd_f_low, sum_cdc_ta_f_low)),
      xdup_on == 1 & xdedup_type == "C" & fdup_dsd_type == "Point" & fdup_ta_type == "Range" ~ pmin(abs(xdedup), p_xdedup*pmin(sum_cdc_dsd_f, sum_cdc_ta_f_low)),
      xdup_on == 1 & xdedup_type == "C" & fdup_dsd_type == "Range" & fdup_ta_type == "Point" ~ pmin(abs(xdedup), p_xdedup*pmin(sum_cdc_dsd_f_low, sum_cdc_ta_f)),
      xdup_on == 1 & xdedup_type == "C" & fdup_dsd_type == "Range" & fdup_ta_type == "Range" ~ pmin(abs(xdedup), p_xdedup*pmin(sum_cdc_dsd_f_low, sum_cdc_ta_f_low)),
      TRUE ~ 0),
    xfdup_low = case_when(
      xdup_on == 1 & xdedup_type == "M" & fdup_dsd_type == "Point" & fdup_ta_type == "Range" ~ pmin(abs(xdedup), pmin(sum_cdc_dsd_f, sum_cdc_ta_f_high)),
      xdup_on == 1 & xdedup_type == "M" & fdup_dsd_type == "Range" & fdup_ta_type == "Point" ~ pmin(abs(xdedup), pmin(sum_cdc_dsd_f_high, sum_cdc_ta_f)),
      xdup_on == 1 & xdedup_type == "M" & fdup_dsd_type == "Range" & fdup_ta_type == "Range" ~ pmin(abs(xdedup), pmin(sum_cdc_dsd_f_high, sum_cdc_ta_f_high)),
      xdup_on == 1 & xdedup_type == "C" & fdup_dsd_type == "Point" & fdup_ta_type == "Range" ~ pmin(abs(xdedup), p_xdedup*pmin(sum_cdc_dsd_f, sum_cdc_ta_f_high)),
      xdup_on == 1 & xdedup_type == "C" & fdup_dsd_type == "Range" & fdup_ta_type == "Point" ~ pmin(abs(xdedup), p_xdedup*pmin(sum_cdc_dsd_f_high, sum_cdc_ta_f)),
      xdup_on == 1 & xdedup_type == "C" & fdup_dsd_type == "Range" & fdup_ta_type == "Range" ~ pmin(abs(xdedup), p_xdedup*pmin(sum_cdc_dsd_f_high, sum_cdc_ta_f_high)),
      TRUE ~ 0)) %>% 
  mutate_if(is.numeric, ~replace(., is.na(.), 0))  

# ============= VII. Site-Level Dedups for OUs different from DATIM ============= 
# Output site-level dedups for OUs to refer to when checking DFPM data
df_sitelevel_dedup <- df_xfdup %>%
  filter(fdup_dsd>0 | fdup_dsd_low>0 | fdup_dsd_high>0 | fdup_ta>0 | fdup_ta_low>0 | fdup_ta_high>0 | fdup_na>0 | fdup_na_low>0 | fdup_na_high>0 | xfdup>0 | xfdup_high>0 | xfdup_low>0) %>%
  mutate(fduped_dsd_max = sum_cdc_dsd - fdup_dsd - fdup_dsd_low,
         fduped_dsd_min = sum_cdc_dsd - fdup_dsd - fdup_dsd_high,
         fduped_ta_max  = sum_cdc_ta  - fdup_ta  - fdup_ta_low,
         fduped_ta_min  = sum_cdc_ta  - fdup_ta  - fdup_ta_high,
         fduped_na_max  = sum_cdc_na  - fdup_na  - fdup_na_low,
         fduped_na_min  = sum_cdc_na  - fdup_na  - fdup_na_high) %>%
  select(period,
         fiscal_year,
         orgunituid,
         operatingunit,
         indicator,
         disaggregate,
         categoryoptioncomboname,
         numeratordenom,
         num_cdc_im_dsd,
         num_ag_im_dsd,
         sum_cdc_dsd,
         fduped_dsd_max,
         fduped_dsd_min,
         num_cdc_im_ta,
         num_ag_im_ta,
         sum_cdc_ta,
         fduped_ta_max,
         fduped_ta_min,
         num_cdc_im_na,
         num_cdc_im_na,
         sum_cdc_na,
         fduped_na_max,
         fduped_na_min,
         xfdup,
         xfdup_high,
         xfdup_low) %>%
  mutate(periodtype = case_when(period %in% c("targets")  ~ "COP",
                                indicator %in% c("TX_CURR","TX_TB") & period %in% c("qtr4","cumulative") ~ "APR",
                                indicator %ni% c("TX_CURR","TX_TB") & period %in% c("qtr1","qtr2","qtr3","qtr4","cumulative")~ "APR",
                                TRUE ~ "X")) %>% 
  filter(periodtype != "X")

write_csv(df_sitelevel_dedup, paste0("ALL_OUs_",timeperiod,"_site_dedupsa.csv"), na="")

# ============= VIII. OU-level Dataset ============= 
df_apr <- df_xfdup %>% 
  select(period,
         fiscal_year,
         operatingunit,
         indicator,
         disaggregate,
         categoryoptioncomboname, 
         numeratordenom,
         sum_cdc_dsd,
         fdup_dsd,
         fdup_dsd_low,
         fdup_dsd_high,
         sum_cdc_ta,
         fdup_ta,
         fdup_ta_low,
         fdup_ta_high,
         sum_cdc_na,
         fdup_na,
         fdup_na_low,
         fdup_na_high,
         xfdup,
         xfdup_high,
         xfdup_low) %>% 
  gather(colvarx, value, sum_cdc_dsd:xfdup_low) %>%  
  filter(!is.na(colvarx)) %>%
  mutate(periodtype = case_when(period %in% c("targets")  ~ "COP",
                                indicator %in% c("TX_CURR","TX_TB") & period %in% c("qtr4","cumulative") ~ "APR",
                                indicator %ni% c("TX_CURR","TX_TB") & period %in% c("qtr1","qtr2","qtr3","qtr4","cumulative")~ "APR",
                                TRUE ~ "X")) %>% 
  filter(periodtype != "X") %>% 
  group_by(fiscal_year, 
           periodtype,
           operatingunit, 
           indicator, 
           disaggregate,
           categoryoptioncomboname, 
           numeratordenom,
           colvarx) %>% 
  summarise(value = sum(value, na.rm=T)) %>%
  ungroup() %>% 
  spread(colvarx, value) %>% 
  mutate_if(is.numeric, ~replace(., is.na(.), 0)) %>%
  bind_rows(
    .[FALSE, FALSE] %>% 
      mutate(sum_cdc_dsd    = 0,
             fdup_dsd       = 0,
             fdup_dsd_low   = 0,
             fdup_dsd_high  = 0,
             fduped_dsd_max = 0,
             fduped_dsd_min = 0,
             sum_cdc_ta     = 0,
             fdup_ta        = 0,
             fdup_ta_low    = 0,
             fdup_ta_high   = 0,
             fduped_ta_max  = 0,
             fduped_ta_min  = 0,
             sum_cdc_na     = 0,
             fdup_na        = 0,
             fdup_na_low    = 0,
             fdup_na_high   = 0,
             fduped_na_max  = 0,
             fduped_na_min  = 0,
             xfdup          = 0,
             xfdup_high     = 0,
             xfdup_low      = 0)) %>% 
  rename(fractionalpart = numeratordenom,
         OU_name        = operatingunit) %>% 
  mutate(fduped_dsd_max = sum_cdc_dsd - fdup_dsd - fdup_dsd_low,
         fduped_dsd_min = sum_cdc_dsd - fdup_dsd - fdup_dsd_high,
         fduped_ta_max  = sum_cdc_ta  - fdup_ta  - fdup_ta_low,
         fduped_ta_min  = sum_cdc_ta  - fdup_ta  - fdup_ta_high,
         fduped_na_max  = sum_cdc_na  - fdup_na  - fdup_na_low,
         fduped_na_min  = sum_cdc_na  - fdup_na  - fdup_na_high) %>% 
  gather(colvar, value, fdup_dsd:fduped_na_min) %>% 
  filter(!is.na(value))

# ============= VIII. Append Original DATIM ============= 
# Restoring the original DATIM values for DSD, TA, and NA.
df_complete <- df %>% 
  mutate(colvar = case_when(
    mech_code %ni% c("00000","00001") & fundingagency=="HHS/CDC" & indicatortype == "DSD" ~ "sum_cdc_dsd",
    mech_code %ni% c("00000","00001") & fundingagency=="HHS/CDC" & indicatortype == "TA"  ~ "sum_cdc_ta",
    mech_code %ni% c("00000","00001") & fundingagency=="HHS/CDC" & indicatortype %ni% c("DSD","TA")  ~ "sum_cdc_na",
    mech_code %ni% c("00000","00001") & fundingagency!="HHS/CDC" & indicatortype == "DSD" ~ "sum_ag_dsd",
    mech_code %ni% c("00000","00001") & fundingagency!="HHS/CDC" & indicatortype == "TA"  ~ "sum_ag_ta",
    mech_code %ni% c("00000","00001") & fundingagency!="HHS/CDC" & indicatortype %ni% c("DSD","TA")  ~ "sum_ag_na")) %>% 
  filter(!is.na(colvar)) %>% 
  select(all_of(vlist)) %>% 
  group_by_if(is.character) %>% 
  summarize(value = sum(value, na.rm=T)) %>%
  ungroup() %>% 
  filter(!is.na(value) & value != 0) %>% 
  mutate(value = abs(value)) %>% 
  filter(colvar %in% c("sum_cdc_dsd","sum_cdc_ta","sum_cdc_na")) %>%
  mutate(periodtype = case_when(period %in% c("targets") ~ "COP",
                                indicator %in% c("TX_CURR","TX_TB") & period %in% c("qtr4","cumulative")                     ~ "APR",
                                indicator %ni% c("TX_CURR","TX_TB") & period %in% c("qtr1","qtr2","qtr3","qtr4","cumulative")~ "APR",
                                TRUE ~ "X")) %>% 
  filter(periodtype != "X") %>% 
  group_by(fiscal_year, 
           periodtype,
           operatingunit, 
           indicator, 
           disaggregate,
           categoryoptioncomboname, 
           numeratordenom,
           colvar) %>% 
  summarise(value = sum(value, na.rm=T)) %>%
  ungroup() %>% 
  spread(colvar, value) %>%
  bind_rows(
    df_apr[FALSE, FALSE] %>% 
      mutate(sum_cdc_dsd = NA,
             sum_cdc_ta  = NA,
             sum_cdc_na  = NA)
  ) %>% 
  mutate(DATIM_NA        = case_when(periodtype %in% c("APR","COP") ~ sum_cdc_na),
         DATIM_DSD       = case_when(periodtype %in% c("APR","COP") ~ sum_cdc_dsd),
         DATIM_TA        = case_when(periodtype %in% c("APR","COP") ~ sum_cdc_ta)) %>%
  rename(fractionalpart  = numeratordenom,
         OU_name         = operatingunit) %>% 
  select(-c(sum_cdc_dsd,sum_cdc_ta,sum_cdc_na)) %>% 
  gather(colvar, value,
         DATIM_NA,
         DATIM_DSD,
         DATIM_TA)%>%
  bind_rows(df_apr) %>%   
  spread(colvar, value) %>% 
  mutate(Dedup_DSD = case_when(fduped_dsd_max == fduped_dsd_min ~ as.character(format(fduped_dsd_max,big.mark = ",",trim=TRUE)),
                               fduped_dsd_max != fduped_dsd_min ~ paste(format(fduped_dsd_min,big.mark=",",trim=TRUE), format(fduped_dsd_max,big.mark=",",trim=TRUE), sep="-")),
         Dedup_TA  = case_when(fduped_ta_max  == fduped_ta_min ~ as.character(format(fduped_ta_max,big.mark=",",trim=TRUE)),
                               fduped_ta_max  != fduped_ta_min ~ paste(format(fduped_ta_min,big.mark=",",trim=TRUE), format(fduped_ta_max,big.mark=",",trim=TRUE), sep="-")),
         Dedup_NA  = case_when(fduped_na_max  == fduped_na_min ~ as.character(format(fduped_na_max,big.mark=",",trim=TRUE)),
                               fduped_na_max  != fduped_na_min ~ paste(format(fduped_na_min,big.mark=",",trim=TRUE), format(fduped_na_max,big.mark=",",trim=TRUE), sep="-"))) %>% 
  mutate(Dedup_Total_max = fduped_dsd_max + fduped_ta_max + fduped_na_max - xfdup - xfdup_low,
         Dedup_Total_min = fduped_dsd_min + fduped_ta_min + fduped_na_min - xfdup - xfdup_high) %>% 
  mutate(Dedup_Total     = case_when(Dedup_Total_max == Dedup_Total_min ~ as.character(format(Dedup_Total_max,big.mark=",",trim=TRUE)),
                                     Dedup_Total_max != Dedup_Total_min ~ as.character(paste(format(Dedup_Total_min,big.mark=",",trim=TRUE), format(Dedup_Total_max,big.mark=",",trim=TRUE), sep="-")))) %>%
  mutate(Xfdup_Total_max = sum_cdc_dsd + sum_cdc_ta + sum_cdc_na - xfdup - xfdup_low,
         Xfdup_Total_min = sum_cdc_dsd + sum_cdc_ta + sum_cdc_na - xfdup - xfdup_high) %>% 
  mutate(Xfdup_Total     = case_when(Xfdup_Total_max == Xfdup_Total_min ~ as.character(format(Xfdup_Total_max,big.mark=",",trim=TRUE)),
                                     Xfdup_Total_max != Xfdup_Total_min ~ as.character(paste(format(Xfdup_Total_min,big.mark = ",",trim=TRUE), format(Xfdup_Total_max,big.mark=",",trim=TRUE), sep="-")))) %>%
  bind_rows(
    .[F, F] %>% 
      mutate(DATIM_DSD   = NA,
             DATIM_TA	   = NA,
             DATIM_NA	   = NA,
             Dedup_Total = NA_character_,
             Dedup_DSD	 = NA_character_,
             Dedup_TA	   = NA_character_,
             Dedup_NA    = NA_character_,
             Xfdup_Total = NA_character_)
  ) %>% 
  select(fiscal_year,
         periodtype,
         OU_name,
         indicator,
         disaggregate,
         categoryoptioncomboname, 
         fractionalpart,
         DATIM_DSD,
         DATIM_TA,
         DATIM_NA,
         Dedup_Total,
         Dedup_DSD,
         Dedup_TA,
         Dedup_NA,
         Xfdup_Total) %>%   
  rowwise() %>%
  mutate(DATIM_Total = format(sum(as.numeric(DATIM_DSD),as.numeric(DATIM_TA),as.numeric(DATIM_NA),na.rm=TRUE),big.mark=",",trim=TRUE)) %>%
  mutate(DATIM_Total = case_when(is.na(DATIM_DSD) & is.na(DATIM_TA)~NA_character_,TRUE~as.character(DATIM_Total))) %>%
  mutate(DATIM_DSD   = if_else(is.na(DATIM_DSD),NA_character_,as.character(format(DATIM_DSD,big.mark = ",",trim=TRUE))),
         DATIM_TA    = if_else(is.na(DATIM_TA),NA_character_,as.character(format(DATIM_TA,big.mark = ",",trim=TRUE))),
         DATIM_NA    = if_else(is.na(DATIM_NA),NA_character_,as.character(format(DATIM_NA,big.mark = ",",trim=TRUE)))) %>% 
  mutate(fiscal_year = if_else(periodtype %in% c("COP"), as.character(as.numeric(fiscal_year)-1), fiscal_year), 
         Dedup_DSD   = case_when(is.na(DATIM_DSD) & Dedup_DSD=="0"  ~ NA_character_,
                                 TRUE  ~ Dedup_DSD), 
         Dedup_TA    = case_when(is.na(DATIM_TA) & Dedup_TA=="0"  ~ NA_character_,
                                 TRUE  ~ Dedup_TA), 
         Dedup_NA    = case_when(is.na(DATIM_NA) & Dedup_NA=="0"  ~ NA_character_,
                                 TRUE  ~ Dedup_NA)) %>% 
  mutate(Dedup_Total = if_else(is.na(Dedup_DSD) & is.na(Dedup_TA) & is.na(Dedup_NA), NA_character_, Dedup_Total),
         Xfdup_Total = if_else(is.na(Dedup_DSD) & is.na(Dedup_TA) & is.na(Dedup_NA), NA_character_, Xfdup_Total)) %>%
  rename(fiscalyear  = fiscal_year) %>%
  group_by_if(is.character) %>% 
  summarise_if(is.numeric, sum, na.rm = TRUE) %>%
  ungroup() %>% 
  filter(!is.na(DATIM_DSD) | !is.na(DATIM_TA) | !is.na(DATIM_NA) | !is.na(DATIM_Total) | 
           !is.na(Dedup_DSD) | !is.na(Dedup_TA) | !is.na(Dedup_NA) | !is.na(Dedup_Total) | !is.na(Xfdup_Total))

# ============= Outputs ============= 
# Raw output for manipulation
write_csv(df_complete, paste0(format(Sys.time(), "%Y%m%dT%H"),"_ALL_OUs_",timeperiod,"_Fdups_rawa.csv"), na="")

# ============= Output for CDS Team ============
# Removing extra variables and data
df_fdup_all_ous <- df_complete %>% 
  filter(!(fiscalyear == "2019" & periodtype == "COP")) %>%
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


# Create FY22 dummy table for 3 CBJ indicators
df_complete <- select(df_fdup_all_ous, Country) %>%
  unique() %>%
  mutate(TX_CURR    = "",
         TB_ART     = "",
         VMMC_CIRC  = "") %>%
  pivot_longer(cols = TX_CURR:VMMC_CIRC) %>%
  select(-value) %>%
  rename(indicator = name)%>%
  mutate(resulttargets  = "targets",
         fiscalyear     = nextyear+1,
         disaggregate   = "Total Numerator",
         fractionalpart = "N",
         indicator      = paste(indicator," (",fractionalpart,")",sep=""),
         DATIM_DSD      = "",
         Dedup_DSD      = "",
         Revised_DSD    = "",
         DATIM_TA       = "",
         Dedup_TA       = "",
         Revised_TA     = "",
         DATIM_Total    = "",
         Dedup_Total    = "",
         Revised_Total  = "") %>%
  select(-fractionalpart) %>%
  bind_rows(
    df_fdup_all_ous
  ) %>%
  ungroup() %>%
  mutate(rownum=dplyr::row_number(),
         filename=paste0(format(Sys.time(), "%Y%m%dT%H"), "_ALL_OUs_", timeperiod, "_Fdupsa.csv"))

# Final export of the dataset for SharePoint *format from 2019- to compare to version below or see if we can incorporate into PowerBI directly using queries
write_csv(df_complete, paste0(format(Sys.time(), "%Y%m%dT%H"),"_ALL_OUs_", timeperiod ,"_Fdupsa.csv"), na="")

# ============= Output for Power BI analytics and dashboards ============
# Get numeric values for analytics, assigning a value for the deduplicate
# that would result in the maximum difference from the DATIM result if it were chose
# Also need to create "Total" values for if we just added DSD, TA - potential 
# double-counting without cross-walk deduplication of DSD and TA xfdup, so we output those to check

df_pbi <- rawoutput %>%
  filter(!(fiscalyear=="2019" & periodtype=="COP")) %>%
  rowwise() %>%
  mutate(DATIM_DSD        = as.numeric(str_remove_all(DATIM_DSD,   ",")),
         DATIM_TA         = as.numeric(str_remove_all(DATIM_TA,    ",")),
         DATIM_NA         = as.numeric(str_remove_all(DATIM_NA,    ",")),
         DATIM_Total      = as.numeric(str_remove_all(DATIM_Total, ","))) %>%
  mutate(Dedup_DSD_Low    = as.numeric(case_when(grepl("-",Dedup_DSD) ~ gsub("-.*$","",str_remove_all(Dedup_DSD,",")),
                                                 TRUE ~ str_remove_all(Dedup_DSD,","))),
         Dedup_DSD_High   = as.numeric(case_when(grepl("-",Dedup_DSD)~gsub(".*-","",str_remove_all(Dedup_DSD,",")),
                                                 TRUE ~ str_remove_all(Dedup_DSD,","))),
         Dedup_TA_Low     = as.numeric(case_when(grepl("-",Dedup_TA) ~ gsub("-.*$","",str_remove_all(Dedup_TA,",")),
                                                 TRUE ~ str_remove_all(Dedup_TA,","))),
         Dedup_TA_High    = as.numeric(case_when(grepl("-",Dedup_TA)~gsub(".*-","",str_remove_all(Dedup_TA,",")),
                                                 TRUE ~ str_remove_all(Dedup_TA,","))),
         Dedup_Total_Low  = as.numeric(case_when(grepl("-",Dedup_Total) ~ gsub("-.*$","",str_remove_all(Dedup_Total,",")),
                                                 TRUE ~ str_remove_all(Dedup_Total,","))),
         Dedup_Total_High = as.numeric(case_when(grepl("-",Dedup_Total)~gsub(".*-","",str_remove_all(Dedup_Total,",")),
                                                 TRUE ~ str_remove_all(Dedup_Total,","))),
         Xfdup_Total_Low  = as.numeric(case_when(grepl("-",Xfdup_Total) ~ gsub("-.*$","",str_remove_all(Xfdup_Total,",")),
                                                 TRUE ~ str_remove_all(Xfdup_Total,","))),
         Xfdup_Total_High = as.numeric(case_when(grepl("-",Xfdup_Total)~gsub(".*-","",str_remove_all(Xfdup_Total,",")),
                                                 TRUE ~ str_remove_all(Xfdup_Total,","))),
         Max_DSD_Dedup    = case_when(abs(DATIM_DSD-Dedup_DSD_Low)>abs(DATIM_DSD-Dedup_DSD_High) ~ Dedup_DSD_Low,
                                      abs(DATIM_DSD-Dedup_DSD_Low)<abs(DATIM_DSD-Dedup_DSD_High) ~ Dedup_DSD_High,
                                      TRUE ~ Dedup_DSD_Low),
         Max_TA_Dedup     = case_when(abs(DATIM_TA-Dedup_TA_Low)>abs(DATIM_TA-Dedup_TA_High) ~ Dedup_TA_Low,
                                      abs(DATIM_TA-Dedup_TA_Low)<abs(DATIM_TA-Dedup_TA_High) ~ Dedup_TA_High,
                                      TRUE ~ Dedup_TA_Low),
         Max_Total_Dedup  = case_when(abs(DATIM_Total-Dedup_Total_Low)>abs(DATIM_Total-Dedup_Total_High) ~ Dedup_Total_Low,
                                      abs(DATIM_Total-Dedup_Total_Low)<abs(DATIM_Total-Dedup_Total_High) ~ Dedup_Total_High,
                                      TRUE ~ Dedup_Total_Low)) %>%
  select(-Dedup_DSD,-Dedup_Total,-Dedup_TA,
         -Dedup_TA_Low,-Dedup_TA_High,
         -Dedup_DSD_Low,-Dedup_DSD_High,-Dedup_Total_High,-Dedup_Total_Low) %>%
  rename(Dedup_DSD=Max_DSD_Dedup,Dedup_TA=Max_TA_Dedup,Dedup_Total=Max_Total_Dedup) %>%
  mutate(Xfdup_Total = NA)

# Final export of the numeric dataset for quality checking algorithm results
write_csv(df_pbi, paste0("ALL_OUs_", timeperiod,"_numeric_fdups.csv"), na="")

#  ======================================== Lab Subset ========================================
df_lab <- df_datim %>%
  filter(indicator == "LAB_PTCQI" & cumulative > 0 & standardizeddisaggregate %in% c("Lab/CQI","POCT/CQI")) %>% 
  mutate(operatingunit = case_when(grepl("region", operatingunit, ignore.case = TRUE) ~ countryname, 
                                   TRUE                                               ~ operatingunit))

df_lab2 <- df_lab %>%
  # CQI by Agency Count
  filter(otherdisaggregate != "Testing with no participation") %>%
  distinct(operatingunit, orgunituid, fiscal_year, fundingagency) %>%
  group_by(operatingunit, fiscal_year, fundingagency) %>%
  summarize(lab_facilities = n()) %>%
  mutate(type = "CQI") %>%
  ungroup() %>%
  # Total by Agency Count
  rbind(
    df_lab %>%
      distinct(operatingunit, orgunituid, fiscal_year, fundingagency) %>%
      group_by(operatingunit, fiscal_year, fundingagency) %>%
      summarize(lab_facilities = n()) %>%
      mutate(type = "Total") %>%
      ungroup()
  ) %>%
  # CQI Overall Count
  rbind(
    df_lab %>%
      filter(otherdisaggregate != "Testing with no participation") %>%
      distinct(operatingunit, fiscal_year, orgunituid) %>%
      group_by(operatingunit, fiscal_year) %>%
      summarize(lab_facilities = n()) %>%
      mutate(type = "CQI", fundingagency = "PEPFAR") %>%
      ungroup()
  ) %>%
  # Total Overall Count
  rbind(
    df_lab %>%
      distinct(operatingunit, fiscal_year, orgunituid) %>%
      group_by(operatingunit, fiscal_year) %>%
      summarize(lab_facilities = n()) %>%
      mutate(type = "Total", fundingagency = "PEPFAR") %>%
      ungroup()
  ) %>%
  spread(type, lab_facilities, fill = 0) %>%
  select(operatingunit, fiscal_year, fundingagency, CQI, Total)

## Lab Output
write_csv(df_lab2, paste0("ALL_OUs_", timeperiod,"_lab_counts.csv"), na="")
