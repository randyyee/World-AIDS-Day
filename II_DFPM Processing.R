## ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
## TITLE: World AIDS Day
## AUTHOR: Randy Yee (pcx5@cdc.gov)
## DESCRIPTION: Import and dedupe MSD
## IMPORTS:
##    - DFPM PowerApp Results
##    - Q4 Clean OUxIM MSD
##    - 
## CREATION DATE: 11/8/2021
## UPDATE: 
## ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

# ============= I. Setup ============= 
library(tidyverse)
library(readxl)
options(scipen=999)


# ============= II. Import DFPM Results ============= 
df_dfpm <- read_excel("D:/PostDFPM/excel_output.xlsx",
                      sheet     = "query",
                      col_names = TRUE,
                      col_types = c("skip", "text",    "text",    "text", "text",    "text", "numeric", "skip", "numeric", "numeric",
                                    "skip", "numeric", "numeric", "skip", "numeric", "skip", "skip",    "skip", "skip",    "skip",
                                    "skip", "skip",    "skip")) %>%
  # Some countries did not revise their totals - fill in those values in the "Revised_Total" column
  rowwise() %>%
  mutate(Revised_Total = case_when(fiscalyear == "2022" & (is.na(Revised_Total) & (!is.na(Revised_DSD) | !is.na(Revised_TA))) ~ sum(Revised_DSD, Revised_TA, na.rm=TRUE),
                                   TRUE ~ Revised_Total)) %>%
  ungroup() %>%
  pivot_longer(DATIM_DSD:Revised_Total) %>%
  separate(name, into = c("source", "sup_type"), sep = "_") %>%
  filter(!is.na(value)) %>%
  mutate(sup_type      = tolower(sup_type),
         source        = tolower(source),
         Country       = case_when(Country == "Cote d Ivoire"  ~ "Cote d'Ivoire", 
                                   TRUE ~ Country),
         resulttargets = case_when(resulttargets == "targets"  ~ "target",
                                   resulttargets == "results"  ~ "result"))


# ============= III. Import DATIM Q4 Clean ============= 
df_datim_clean_q4 <- read_delim(file = "C:/Users/ouo8/OneDrive - CDC/HIDMSB/WAD/WAD 2020/MSDs/Genie-OUByIMs-Global-Daily-2020-12-28.zip", 
                       "\t", 
                       escape_double = FALSE,
                       trim_ws   = TRUE,
                       col_types = cols(.default   = col_character(), 
                                        targets    = col_double(),
                                        qtr1       = col_double(),
                                        qtr2       = col_double(),
                                        qtr3       = col_double(),
                                        qtr4       = col_double(),
                                        cumulative = col_double())) 

# ============= IV. Subset DATIM Q4 Clean-CDC ============= 
df_datim_cdc <- df_datim_clean_q4 %>%
  filter(fundingagency == "HHS/CDC") %>%
  mutate(source = "datimclean",
         sup_type = if_else(indicatortype %in% c("TA", "DSD"), tolower(indicatortype), "na"),
         operatingunit = countryname) %>% 
  select(operatingunit, 
         indicator, 
         disaggregate, 
         numeratordenom, 
         fiscal_year,  
         sup_type, 
         source, 
         cumulative, 
         targets) %>% 
  pivot_longer(cols = cumulative:targets, names_to = "period", values_to = "value", values_drop_na = TRUE) %>% 
  filter((fiscal_year == "2020" & period == "cumulative") | (fiscal_year == "2021" & period == "targets")) %>%
  group_by_if(is.character) %>% 
  summarize(value = sum(value, na.rm = TRUE)) %>% 
  ungroup() %>% 
  mutate(resulttargets = case_when(period == "targets"     ~ "target",
                                   period == "cumulative"  ~ "result"),
         indicator     = paste0(indicator," (", numeratordenom,")")) %>%
  rename(fiscalyear    = fiscal_year) %>%
  select(-period, -numeratordenom)

df_datim_cdc <- df_datim_cdc %>%
  select(-sup_type) %>% 
  group_by_if(is.character) %>% 
  summarize(value = sum(value, na.rm = TRUE)) %>% 
  ungroup() %>% 
  mutate(sup_type = "total") %>%
  bind_rows(
    df_datim_cdc
  ) %>%
  rename(Country = operatingunit)


# ============= V. Merge DFPM with DATIM Q4 Clean ============= 
#compare datim and dfpm values
compare_datim_sp <- bind_rows(datim_dfpm_cdc,cdc_q4clean_all) %>%
  # need to create the "adj" column, which is equal to "Y" if Revised column is not NA (i.e. data was entered in the Power App)
  pivot_wider(names_from = source,values_from = value) %>% 
  mutate(adj = case_when(is.na(revised) ~ "N",
                       TRUE ~ "Y")) 


#check for any cases where there was data in the pre-clean data, and not in the cleaned data
View(filter(compare_datim_sp,(is.na(datim) & !is.na(datimclean)) | (is.na(datimclean) & !is.na(datim))))


#create new "final" columns for DFPM + precleaned, DFPM+cleaned

cdc_out<-compare_datim_sp %>%
  #calculate the difference with the original datim values, and then create a new 
  #variable replacing values that were not adjusted during DFPM with cleaned datim values
  mutate(new=if_else(adj=="Y",revised,datim),
         newclean=if_else(adj=="Y",revised,datimclean)) %>%
  #remove revised column
  select(-revised)

# ============= VI. Export Comparison ============= 
write_csv(cdc_out,"C:/Users/ouo8/OneDrive - CDC/HIDMSB/WAD 2020/Output/compare_datim_sp.csv")


# format for output to Power BI dashboard
df_final <- cdc_out %>% 
  #replace Tanzania APR values with original datim values because of large discrepancies
  mutate(fundingagency="CDC",
         source="new",
         type = case_when(resulttargets %in% c("target")  ~ "COP",
                          resulttargets %in% c("result")  ~ "APR",
                          TRUE  ~ "X")) %>%   # catchall for values not COP or APR
  # change meaning of "fiscalyear" column to match definition used in prior years (referring to year of COP for targets)
  rename(fyear         = fiscalyear) %>%
  mutate(wad           = datim,
         fiscalyear    = if_else(type=="COP", as.character(as.numeric(fyear)-1), as.character(fyear)),
         period        = paste(fyear, resulttargets, sep="_"),
         OperatingUnit = Country) %>% 
  select(fiscalyear,
         type,
         OperatingUnit,
         indicator,
         disaggregate,
         resulttargets,
         fyear,
         period,
         adj,
         fundingagency,
         sup_type,
         new,
         newclean,
         wad,
         datimclean,
         source) %>%
  group_by_if(is.character) %>% 
  summarize(new = sum(new, na.rm = T), 
            newclean=sum(newclean,na.rm=T), 
            wad=sum(wad,na.rm=T),
            datimclean=sum(datimclean,na.rm=T)) %>% 
  ungroup() %>%
  pivot_longer(cols = new:datimclean, names_to = "mer_source", values_to = "val") %>%
  #remove one set of fy22 targets to avoid duplication
  filter(!(mer_source %in% c("wad", "datimclean", "newclean") & period == "2022_target"))



# ============= VII. Import PEPFAR Prelim (PreClean DATIM Q4) ============= 
df_dfpm_PEPFAR <- read_csv(file = "C:/Users/ouo8/OneDrive - CDC/HIDMSB/WAD/WAD 2020/Output/PowerBI_FY20Q4_precleanWAD_prelim_pepfar.csv",
                 col_names = TRUE,
                 col_types = cols(operatingunit           = "c",
                                  indicator               = "c",
                                  disaggregate            = "c",
                                  categoryoptioncomboname = "c",
                                  fractionalpart          = "c",
                                  sup_type                = "c",
                                  fyear                   = "c",
                                  fundingagency           = "c",
                                  adj                     = "c",
                                  result                  = "d",
                                  target                  = "d")) %>%
  rename(OperatingUnit = operatingunit) %>%
  pivot_longer(result:target, names_to="resulttargets", values_to="val", values_drop_na = TRUE) %>%
  filter(!(resulttargets == "target" & fyear == "2020")) %>%
  mutate(type       = case_when(resulttargets == "result" ~ "APR",
                                resulttargets == "target" ~ "COP"),
         fiscalyear = if_else(type=="COP", as.character(as.numeric(fyear)-1), as.character(fyear)),
         source     = "datim",
         mer_source = "new",
         period     = paste(fyear, resulttargets, sep="_"),
         sup_type   = tolower(sup_type),
         indicator  = paste0(indicator, " (", fractionalpart, ")")) %>%
  select(-categoryoptioncomboname, -fractionalpart)

# ============= VIII. Subset DATIM Q4 Clean-PEPFAR ============= 
df_datim_pepfar <- df_datim_clean_q4 %>%
  mutate(sup_type      = if_else(indicatortype %in% c("TA","DSD"),tolower(indicatortype),"na"),
         operatingunit = countryname) %>% 
  select(operatingunit, 
         indicator, 
         disaggregate, 
         numeratordenom, 
         fiscal_year,  
         sup_type, 
         source, 
         cumulative, 
         targets) %>% 
  pivot_longer(cols      = cumulative:targets,
               names_to  = "period",
               values_to = "value",
               values_drop_na = TRUE) %>% 
  rename(fyear = fiscal_year) %>%
  filter((fyear == "2020" & period == "cumulative") | (fyear == "2021" & period == "targets")) %>%  
  group_by_if(is.character) %>% 
  summarize(val = sum(value, na.rm = TRUE)) %>% 
  ungroup() %>% 
  mutate(type          = case_when(period == "targets"     ~ "COP",
                                   period == "cumulative"  ~ "APR"),
         resulttargets = case_when(type   == "COP"         ~ "target", 
                                   type   == "APR"         ~ "result"),
         fiscalyear    = case_when(type   == "APR"         ~ fyear,
                                   type   == "COP"         ~ as.character(as.numeric(fyear)-1)),
         fundingagency = "PEPFAR",
         adj           = "X",
         source        = "datim",
         mer_source    = "newclean",
         period        = paste(fyear, resulttargets, sep="_"),
         indicator     = paste0(indicator, " (", numeratordenom, ")")) %>%
  select(-numeratordenom) %>%
  rename(OperatingUnit = operatingunit)

df_datim_pepfar <- df_datim_pepfar %>%
  select(-sup_type) %>% 
  group_by_if(is.character) %>% 
  summarize(val = sum(val, na.rm = TRUE)) %>% 
  ungroup() %>% 
  mutate(sup_type="total") %>%
  bind_rows(df_datim_pepfar)

output_final<-bind_rows(df_final,PEPFAR,pepfar_clean) %>%
  filter(disaggregate %in% c("Total Numerator", "Total Denominator")) %>%
  filter(!(type == "COP" & fyear == "2021" & val==0))

# ============= IX. Export ============= 
write_csv(output_final, paste("C:/Users/ouo8/OneDrive - CDC/HIDMSB/WAD/WAD 2020/Output/","2020_DFPM_Output_Current_",Sys.Date(),".csv", sep=""), na="")


