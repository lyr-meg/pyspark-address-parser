#!/bin/bash
set -e

# Beeline connection
beeline_connect="jdbc:hive2://sdpsvrwm0124.scglobal.ad.scotiacapital.com:2181,sdpsvrwm0128.scglobal.ad.scotiacapital.com:2181,sdpsvrwm0162.scglobal.ad.scotiacapital.com:2181,sdpsvrwm0123.scglobal.ad.scotiacapital.com:2181,sdpsvrwm0127.scglobal.ad.scotiacapital.com:2181/default;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=hiveserver2"
bee="/usr/bin/beeline -u ${beeline_connect}"

$bee -e "
set hive.exec.parallel=true;
set hive.vectorized.execution.enabled=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.optimize.sort.dynamic.partition=true;

drop table if exists caz_amltm_secure.symcor_payors_parsed;
create table caz_amltm_secure.symcor_payors_parsed as select * from (select * from caz_amltm_secure.ml_symcor_parsed_d2p2 a union all select * from caz_amltm_secure.ml_symcor_parsed_d3p2 b union all select * from caz_amltm_secure.ml_symcor_parsed_d4p3 c) combined;
"

echo "----------------------------------------------------"
echo "$(date) : EDL Table caz_amltm_secure.symcor_payors_parsed Created..."
echo "----------------------------------------------------"


$bee -e "
set hive.exec.parallel=true;
set hive.vectorized.execution.enabled=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.optimize.sort.dynamic.partition=true;

drop table if exists caz_amltm_opendata.symcor_payors_parsed;
create table caz_amltm_opendata.symcor_payors_parsed as select p.*, t.sender_acct_no, t.sender_br_tr, t.sender_institution, t.receiver_acct_no, t.receiver_br_tr, t.receiver_institution from caz_amltm_secure.symcor_payor_parsed p left join caz_amltm_opendata.mds_scotia_s1_cheque t on substring(t.isn,-10)=p.isn and regexp_replace(t.business_date, '-', '')=p.business_date;
"

echo "----------------------------------------------------"
echo "$(date) : EDL Table caz_amltm_opendata.symcor_payors_parsed Created..."
echo "----------------------------------------------------"