pip install awscli

echo "run_date is "
run_date=$(date -d "$1 -2 day" +%Y%m%d)
echo $run_date

DAY1=$(date -d "$run_date -0 day" +%Y%m%d)
DAY2=$(date -d "$run_date -1 day" +%Y%m%d)
DAY3=$(date -d "$run_date -2 day" +%Y%m%d)
DAY4=$(date -d "$run_date -3 day" +%Y%m%d)
DAY5=$(date -d "$run_date -4 day" +%Y%m%d)
DAY6=$(date -d "$run_date -5 day" +%Y%m%d)
DAY7=$(date -d "$run_date -6 day" +%Y%m%d)
DAY8=$(date -d "$run_date -7 day" +%Y%m%d)
DAY9=$(date -d "$run_date -8 day" +%Y%m%d)
DAY10=$(date -d "$run_date -9 day" +%Y%m%d)
DAY11=$(date -d "$run_date -10 day" +%Y%m%d)
DAY12=$(date -d "$run_date -11 day" +%Y%m%d)
DAY13=$(date -d "$run_date -12 day" +%Y%m%d)
DAY14=$(date -d "$run_date -13 day" +%Y%m%d)
DAY15=$(date -d "$run_date -14 day" +%Y%m%d)
DAY16=$(date -d "$run_date -15 day" +%Y%m%d)
DAY17=$(date -d "$run_date -16 day" +%Y%m%d)
DAY18=$(date -d "$run_date -17 day" +%Y%m%d)
DAY19=$(date -d "$run_date -18 day" +%Y%m%d)
DAY20=$(date -d "$run_date -19 day" +%Y%m%d)
DAY21=$(date -d "$run_date -20 day" +%Y%m%d)
DAY22=$(date -d "$run_date -21 day" +%Y%m%d)
DAY23=$(date -d "$run_date -22 day" +%Y%m%d)
DAY24=$(date -d "$run_date -23 day" +%Y%m%d)
DAY25=$(date -d "$run_date -24 day" +%Y%m%d)
DAY26=$(date -d "$run_date -25 day" +%Y%m%d)
DAY27=$(date -d "$run_date -26 day" +%Y%m%d)
DAY28=$(date -d "$run_date -27 day" +%Y%m%d)
DAY29=$(date -d "$run_date -28 day" +%Y%m%d)
DAY30=$(date -d "$run_date -29 day" +%Y%m%d)


echo $DAY1
echo $DAY2
echo $DAY3
echo $DAY4
echo $DAY5
echo $DAY6
echo $DAY7
echo $DAY8
echo $DAY9
echo $DAY10
echo $DAY11
echo $DAY12
echo $DAY13
echo $DAY14
echo $DAY15
echo $DAY16
echo $DAY17
echo $DAY18
echo $DAY19
echo $DAY20
echo $DAY21
echo $DAY22
echo $DAY23
echo $DAY24
echo $DAY25
echo $DAY26
echo $DAY27
echo $DAY28
echo $DAY29
echo $DAY30


datedpi=$(date -d "$run_date -0 day" +%Y%m%d)
file1=`aws s3 ls s3://aiqx/uns_modularize/master_data/dayserial_numeric=$datedpi/ | wc -l`
file2=`aws s3 ls s3://aiqx/uns_modularize/es_indexing/aud_segments/dayserial_numeric=$datedpi/ | wc -l`
file3=`aws s3 ls s3://aiqx/uns_modularize/es_indexing/domains/dayserial_numeric=$datedpi/ | wc -l`
file4=`aws s3 ls s3://aiqx/uns_modularize/es_indexing/words/dayserial_numeric=$datedpi/ | wc -l`
file5=`aws s3 ls s3://aiqx/uns_modularize/es_indexing/adv_segments/dayserial_numeric=$datedpi/ | wc -l`
file6=`aws s3 ls s3://aiqx/uns_modularize/athena_users/cities/dayserial_numeric=$datedpi/ | wc -l`
file7=`aws s3 ls s3://aiqx/uns_modularize/athena_users/domains/dayserial_numeric=$datedpi/ | wc -l`
file8=`aws s3 ls s3://aiqx/uns_modularize/athena_users/audience_segs/dayserial_numeric=$datedpi/ | wc -l`
file9=`aws s3 ls s3://aiqx/uns_modularize/athena_users/keywords/dayserial_numeric=$datedpi/ | wc -l`
file10=`aws s3 ls s3://aiqx/uns_modularize/es_indexing/demog_segs/dayserial_numeric=$datedpi/ | wc -l`
file11=`aws s3 ls s3://aiqx/uns_modularize/es_indexing/devices/dayserial_numeric=$datedpi/ | wc -l`
file12=`aws s3 ls s3://aiqx/uns_modularize/es_indexing/brands/dayserial_numeric=$datedpi/ | wc -l`
file13=`aws s3 ls s3://aiqx/uns_modularize/athena_users/devices/dayserial_numeric=$datedpi/ | wc -l`
file14=`aws s3 ls s3://aiqx/uns_modularize/athena_users/hours/dayserial_numeric=$datedpi/ | wc -l`
file15=`aws s3 ls s3://aiqx/uns_modularize/athena_users/days/dayserial_numeric=$datedpi/ | wc -l`
file16=`aws s3 ls s3://aiqx/uns_modularize/athena_users/demog_segs/dayserial_numeric=$datedpi/ | wc -l`
file17=`aws s3 ls s3://aiqx/uns_modularize/es_indexing/geos/dayserial_numeric=$datedpi/ | wc -l`
file18=`aws s3 ls s3://aiqx/uns_modularize/athena_users/url_words/dayserial_numeric=$datedpi/ | wc -l`
file19=`aws s3 ls s3://aiqx/uns_modularize/metadata/audience_meta_data_final/dayserial_numeric=$datedpi/ | wc -l`
file20=`aws s3 ls s3://aiqx/uns_modularize/metadata/domains_meta_data_final/dayserial_numeric=$datedpi/ | wc -l`
file21=`aws s3 ls s3://aiqx/uns_modularize/metadata/words_meta_data_final/dayserial_numeric=$datedpi/ | wc -l`
file22=`aws s3 ls s3://aiqx/uns_modularize/es_indexing/dayserial_numeric=$datedpi/url_words/ | wc -l`
file23=`aws s3 ls s3://aiqx/uns_modularize/athena_users/networks/dayserial_numeric=$datedpi/ | wc -l`
file24=`aws s3 ls s3://aiqx/uns_modularize/athena_users/titles/dayserial_numeric=$datedpi/ | wc -l`
file25=`aws s3 ls s3://aiqx/uns_modularize/athena_users/cast_day_hour/dayserial_numeric=$datedpi/ | wc -l`
file26=`aws s3 ls s3://aiqx/uns_modularize/athena_users/cast_commercial_brands_titles/dayserial_numeric=$datedpi/ | wc -l`
file27=`aws s3 ls s3://aiqx/uns_modularize/athena_users/cast_commercial_brands_networks/dayserial_numeric=$datedpi/ | wc -l`
file28=`aws s3 ls s3://aiqx/uns_modularize/es_indexing/cast_commercial_brands_list/dayserial_numeric=$datedpi/ | wc -l`
file29=`aws s3 ls s3://aiqx/uns_modularize/athena_users/domains_category/dayserial_numeric=$datedpi/ | wc -l`

i=-1
if [ $file1 -gt 0 ] && [ $file2 -gt 0 ] && [ $file3 -gt 0 ] && [ $file4 -gt 0 ] && [ $file5 -gt 0 ] && [ $file6 -gt 0 ] && [ $file7 -gt 0 ] && [ $file8 -gt 0 ] && [ $file9 -gt 0 ] && [ $file10 -gt 0 ] && [ $file11 -gt 0 ] && [ $file12 -gt 0 ] && [ $file13 -gt 0 ] && [ $file14 -gt 0 ] && [ $file15 -gt 0 ] && [ $file16 -gt 0 ] && [ $file17 -gt 0 ] && [ $file18 -gt 0 ] && [ $file19 -gt 0 ] && [ $file20 -gt 0 ] && [ $file21 -gt 0 ] && [ $file22 -gt 0 ] && [ $file23 -gt 0 ] && [ $file24 -gt 0 ] && [ $file25 -gt 0 ] && [ $file26 -gt 0 ] && [ $file27 -gt 0 ] && [ $file28 -gt 0 ] && [ $file29 -gt 0 ]
	then echo "got in first run"
else
	while [ $i -gt -60 ]
	do
		datedpi=$(date -d "$run_date $i day" +%Y%m%d)
		echo $datedpi
		file1=`aws s3 ls s3://aiqx/uns_modularize/master_data/dayserial_numeric=$datedpi/ | wc -l`
		file2=`aws s3 ls s3://aiqx/uns_modularize/es_indexing/aud_segments/dayserial_numeric=$datedpi/ | wc -l`
		file3=`aws s3 ls s3://aiqx/uns_modularize/es_indexing/domains/dayserial_numeric=$datedpi/ | wc -l`
		file4=`aws s3 ls s3://aiqx/uns_modularize/es_indexing/words/dayserial_numeric=$datedpi/ | wc -l`
		file5=`aws s3 ls s3://aiqx/uns_modularize/es_indexing/adv_segments/dayserial_numeric=$datedpi/ | wc -l`
		file6=`aws s3 ls s3://aiqx/uns_modularize/athena_users/cities/dayserial_numeric=$datedpi/ | wc -l`
		file7=`aws s3 ls s3://aiqx/uns_modularize/athena_users/domains/dayserial_numeric=$datedpi/ | wc -l`
		file8=`aws s3 ls s3://aiqx/uns_modularize/athena_users/audience_segs/dayserial_numeric=$datedpi/ | wc -l`
		file9=`aws s3 ls s3://aiqx/uns_modularize/athena_users/keywords/dayserial_numeric=$datedpi/ | wc -l`
		file10=`aws s3 ls s3://aiqx/uns_modularize/es_indexing/demog_segs/dayserial_numeric=$datedpi/ | wc -l`
		file11=`aws s3 ls s3://aiqx/uns_modularize/es_indexing/devices/dayserial_numeric=$datedpi/ | wc -l`
		file12=`aws s3 ls s3://aiqx/uns_modularize/es_indexing/brands/dayserial_numeric=$datedpi/ | wc -l`
		file13=`aws s3 ls s3://aiqx/uns_modularize/athena_users/devices/dayserial_numeric=$datedpi/ | wc -l`
		file14=`aws s3 ls s3://aiqx/uns_modularize/athena_users/hours/dayserial_numeric=$datedpi/ | wc -l`
		file15=`aws s3 ls s3://aiqx/uns_modularize/athena_users/days/dayserial_numeric=$datedpi/ | wc -l`
		file16=`aws s3 ls s3://aiqx/uns_modularize/athena_users/demog_segs/dayserial_numeric=$datedpi/ | wc -l`
		file17=`aws s3 ls s3://aiqx/uns_modularize/es_indexing/geos/dayserial_numeric=$datedpi/ | wc -l`
		file18=`aws s3 ls s3://aiqx/uns_modularize/athena_users/url_words/dayserial_numeric=$datedpi/ | wc -l`
		file19=`aws s3 ls s3://aiqx/uns_modularize/metadata/audience_meta_data_final/dayserial_numeric=$datedpi/ | wc -l`
		file20=`aws s3 ls s3://aiqx/uns_modularize/metadata/domains_meta_data_final/dayserial_numeric=$datedpi/ | wc -l`
		file21=`aws s3 ls s3://aiqx/uns_modularize/metadata/words_meta_data_final/dayserial_numeric=$datedpi/ | wc -l`
		file22=`aws s3 ls s3://aiqx/uns_modularize/es_indexing/dayserial_numeric=$datedpi/url_words/ | wc -l`
		file23=`aws s3 ls s3://aiqx/uns_modularize/athena_users/networks/dayserial_numeric=$datedpi/ | wc -l`
		file24=`aws s3 ls s3://aiqx/uns_modularize/athena_users/titles/dayserial_numeric=$datedpi/ | wc -l`
		file25=`aws s3 ls s3://aiqx/uns_modularize/athena_users/cast_day_hour/dayserial_numeric=$datedpi/ | wc -l`
		file26=`aws s3 ls s3://aiqx/uns_modularize/athena_users/cast_commercial_brands_titles/dayserial_numeric=$datedpi/ | wc -l`
		file27=`aws s3 ls s3://aiqx/uns_modularize/athena_users/cast_commercial_brands_networks/dayserial_numeric=$datedpi/ | wc -l`
		file28=`aws s3 ls s3://aiqx/uns_modularize/es_indexing/cast_commercial_brands_list/dayserial_numeric=$datedpi/ | wc -l`
		file29=`aws s3 ls s3://aiqx/uns_modularize/athena_users/domains_category/dayserial_numeric=$datedpi/ | wc -l`

		i=$((i-1))
		if [ $file1 -gt 0 ] && [ $file2 -gt 0 ] && [ $file3 -gt 0 ] && [ $file4 -gt 0 ] && [ $file5 -gt 0 ] && [ $file6 -gt 0 ] && [ $file7 -gt 0 ] && [ $file8 -gt 0 ] && [ $file9 -gt 0 ] && [ $file10 -gt 0 ] && [ $file11 -gt 0 ] && [ $file12 -gt 0 ] && [ $file13 -gt 0 ] && [ $file14 -gt 0 ] && [ $file15 -gt 0 ] && [ $file16 -gt 0 ] && [ $file17 -gt 0 ] && [ $file18 -gt 0 ] && [ $file19 -gt 0 ] && [ $file20 -gt 0 ] && [ $file21 -gt 0 ] && [ $file22 -gt 0 ] && [ $file23 -gt 0 ] && [ $file24 -gt 0 ] && [ $file25 -gt 0 ] && [ $file26 -gt 0 ] && [ $file27 -gt 0 ] && [ $file28 -gt 0 ] && [ $file29 -gt 0 ]
			then echo "$i"
			break
		fi
	done
fi
echo "date variable considered for uns daily job"
echo "$datedpi"


copying_date=$1

aws s3 cp s3://aiqx/uns_modularize/es_indexing/dayserial_numeric=$datedpi/url_words/ s3://aiqx/uns_modularize/automated-daily/es_indexing/dayserial_numeric=$copying_date/url_words/ --recursive
aws s3 cp s3://aiqx/uns_modularize/athena_users/url_words/dayserial_numeric=$datedpi/ s3://aiqx/uns_modularize/automated-daily/athena_users/url_words/dayserial_numeric=$copying_date/ --recursive
aws s3 cp s3://aiqx/uns_modularize/athena_users/demog_segs/dayserial_numeric=$datedpi/ s3://aiqx/uns_modularize/automated-daily/athena_users/demog_segs/dayserial_numeric=$copying_date/ --recursive
aws s3 cp s3://aiqx/uns_modularize/athena_users/days/dayserial_numeric=$datedpi/ s3://aiqx/uns_modularize/automated-daily/athena_users/days/dayserial_numeric=$copying_date/ --recursive
aws s3 cp s3://aiqx/uns_modularize/athena_users/hours/dayserial_numeric=$datedpi/ s3://aiqx/uns_modularize/automated-daily/athena_users/hours/dayserial_numeric=$copying_date/ --recursive
aws s3 cp s3://aiqx/uns_modularize/athena_users/devices/dayserial_numeric=$datedpi/ s3://aiqx/uns_modularize/automated-daily/athena_users/devices/dayserial_numeric=$copying_date/ --recursive
aws s3 cp s3://aiqx/uns_modularize/athena_users/keywords/dayserial_numeric=$datedpi/ s3://aiqx/uns_modularize/automated-daily/athena_users/keywords/dayserial_numeric=$copying_date/ --recursive
aws s3 cp s3://aiqx/uns_modularize/athena_users/audience_segs/dayserial_numeric=$datedpi/ s3://aiqx/uns_modularize/automated-daily/athena_users/audience_segs/dayserial_numeric=$copying_date/ --recursive
aws s3 cp s3://aiqx/uns_modularize/athena_users/domains/dayserial_numeric=$datedpi/ s3://aiqx/uns_modularize/automated-daily/athena_users/domains/dayserial_numeric=$copying_date/ --recursive
aws s3 cp s3://aiqx/uns_modularize/athena_users/cities/dayserial_numeric=$datedpi/ s3://aiqx/uns_modularize/automated-daily/athena_users/cities/dayserial_numeric=$copying_date/ --recursive
aws s3 cp s3://aiqx/uns_modularize/es_indexing/geos/dayserial_numeric=$datedpi/ s3://aiqx/uns_modularize/automated-daily/es_indexing/geos/dayserial_numeric=$copying_date/ --recursive
aws s3 cp s3://aiqx/uns_modularize/es_indexing/brands/dayserial_numeric=$datedpi/ s3://aiqx/uns_modularize/automated-daily/es_indexing/brands/dayserial_numeric=$copying_date/ --recursive
aws s3 cp s3://aiqx/uns_modularize/es_indexing/devices/dayserial_numeric=$datedpi/ s3://aiqx/uns_modularize/automated-daily/es_indexing/devices/dayserial_numeric=$copying_date/ --recursive
aws s3 cp s3://aiqx/uns_modularize/es_indexing/words/dayserial_numeric=$datedpi/ s3://aiqx/uns_modularize/automated-daily/es_indexing/words/dayserial_numeric=$copying_date/ --recursive
aws s3 cp s3://aiqx/uns_modularize/es_indexing/domains/dayserial_numeric=$datedpi/ s3://aiqx/uns_modularize/automated-daily/es_indexing/domains/dayserial_numeric=$copying_date/ --recursive
aws s3 cp s3://aiqx/uns_modularize/es_indexing/aud_segments/dayserial_numeric=$datedpi/ s3://aiqx/uns_modularize/automated-daily/es_indexing/aud_segments/dayserial_numeric=$copying_date/ --recursive
aws s3 cp s3://aiqx/uns_modularize/es_indexing/demog_segs/dayserial_numeric=$datedpi/ s3://aiqx/uns_modularize/automated-daily/es_indexing/demog_segs/dayserial_numeric=$copying_date/ --recursive

aws s3 cp s3://aiqx/uns_modularize/metadata/audience_meta_data_final/dayserial_numeric=$datedpi/ s3://aiqx/uns_modularize/automated-daily/metadata/audience_meta_data_final/dayserial_numeric=$copying_date/ --recursive
aws s3 cp s3://aiqx/uns_modularize/metadata/domains_meta_data_final/dayserial_numeric=$datedpi/ s3://aiqx/uns_modularize/automated-daily/metadata/domains_meta_data_final/dayserial_numeric=$copying_date/ --recursive
aws s3 cp s3://aiqx/uns_modularize/metadata/words_meta_data_final/dayserial_numeric=$datedpi/ s3://aiqx/uns_modularize/automated-daily/metadata/words_meta_data_final/dayserial_numeric=$copying_date/ --recursive
aws s3 cp s3://aiqx/uns_modularize/athena_users/networks/dayserial_numeric=$datedpi/ s3://aiqx/uns_modularize/automated-daily/athena_users/networks/dayserial_numeric=$copying_date/ --recursive
aws s3 cp s3://aiqx/uns_modularize/athena_users/titles/dayserial_numeric=$datedpi/ s3://aiqx/uns_modularize/automated-daily/athena_users/titles/dayserial_numeric=$copying_date/ --recursive
aws s3 cp s3://aiqx/uns_modularize/athena_users/cast_day_hour/dayserial_numeric=$datedpi/ s3://aiqx/uns_modularize/automated-daily/athena_users/cast_day_hour/dayserial_numeric=$copying_date/ --recursive
aws s3 cp s3://aiqx/uns_modularize/athena_users/cast_commercial_brands_titles/dayserial_numeric=$datedpi/ s3://aiqx/uns_modularize/automated-daily/athena_users/cast_commercial_brands_titles/dayserial_numeric=$copying_date/ --recursive
aws s3 cp s3://aiqx/uns_modularize/athena_users/cast_commercial_brands_networks/dayserial_numeric=$datedpi/ s3://aiqx/uns_modularize/automated-daily/athena_users/cast_commercial_brands_networks/dayserial_numeric=$copying_date/ --recursive
aws s3 cp s3://aiqx/uns_modularize/es_indexing/cast_commercial_brands_list/dayserial_numeric=$datedpi/ s3://aiqx/uns_modularize/automated-daily/es_indexing/cast_commercial_brands_list/dayserial_numeric=$copying_date/ --recursive
aws s3 cp s3://aiqx/uns_modularize/athena_users/domains_category/dayserial_numeric=$datedpi/ s3://aiqx/uns_modularize/automated-daily/athena_users/domains_category/dayserial_numeric=$copying_date/ --recursive



/usr/lib/hive1.2/usr-bin/hive1.2 -f s3://aiqdatabucket/scripts/aiqx/uns_qubole/aiqx_daily_airflow/basic_load_tables.sql -d DAYSERIAL_NUMERIC=$1 -d datedpi=$datedpi -d DAY1=$DAY1 -d DAY2=$DAY2 -d DAY3=$DAY3 -d DAY4=$DAY4 -d DAY5=$DAY5 -d DAY6=$DAY6 -d DAY7=$DAY7 -d DAY8=$DAY8 -d DAY9=$DAY9 -d DAY10=$DAY10 -d DAY11=$DAY11 -d DAY12=$DAY12 -d DAY13=$DAY13 -d DAY14=$DAY14 -d DAY15=$DAY15 -d DAY16=$DAY16 -d DAY17=$DAY17 -d DAY18=$DAY18 -d DAY19=$DAY19 -d DAY20=$DAY20 -d DAY21=$DAY21 -d DAY22=$DAY22 -d DAY23=$DAY23 -d DAY24=$DAY24 -d DAY25=$DAY25 -d DAY26=$DAY26 -d DAY27=$DAY27 -d DAY28=$DAY28 -d DAY29=$DAY29 -d DAY30=$DAY30


copying_date=$1

file2=`aws s3 ls s3://aiqx/uns_modularize/automated-daily/es_indexing/dayserial_numeric=$copying_date/url_words/ | wc -l`


if [ $file2 -gt 0 ]
then
	aws s3 rm s3://aiqx/uns_modularize/automated-daily/es_indexing/latest/url_words/ --recursive
	aws s3 cp s3://aiqx/uns_modularize/automated-daily/es_indexing/dayserial_numeric=$copying_date/url_words/ s3://aiqx/uns_modularize/automated-daily/es_indexing/latest/url_words/ --recursive
fi
