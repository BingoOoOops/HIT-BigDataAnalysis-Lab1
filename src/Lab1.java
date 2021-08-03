/*
 * by LB
 * in Harbin Institute of Technology
 *
 * 2021 Spring Semester
 */

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;


public class Lab1  {
    /*
    ratingAvgMap: a map of <attribute, Average>.  Average record the average rating of records with the same attribute value

    avgNation: a map of <nationality, Average>.  Average record the average income of records with the same nationality
    avgCareer: a map of <career, Average>.   Average record the average income of records with the same career
     */
    private static Double minRating = Double.MAX_VALUE;
    private static Double maxRating = Double.MIN_VALUE;

    private static int incomeDivision = 3247;
    private static Double longitudeDivision = 9.6727262;
    private static Double latitudeDivision = 57.1664983;
    private static Double altitudeDivision = 42.3169775;

    private static final Map<String, Average> ratingAvgMap = new HashMap<String, Average>(){{
        put("lowIn", new Average("0", "0"));
        put("highIn", new Average("0", "0"));
        put("west", new Average("0", "0"));
        put("east", new Average("0", "0"));
        put("south", new Average("0", "0"));
        put("north", new Average("0", "0"));
        put("lowAlt", new Average("0", "0"));
        put("highAlt", new Average("0", "0"));

    }};
    private static final Map<String, Average> avgNation = new HashMap<>();
    private static final Map<String, Average> avgCareer = new HashMap<>();


    //these are all in history version which is replaced by ratingAvgMap above
//        private BigDecimal avgRatingOfHighIncome = new BigDecimal(0);
//        private BigDecimal couRatingOfHighIncome = new BigDecimal(0);
//        private BigDecimal avgRatingOfLowIncome = new BigDecimal(0);
//        private BigDecimal couRatingOfLowIncome = new BigDecimal(0);
//
//        private BigDecimal avgRatingOfWest = new BigDecimal(0);
//        private BigDecimal couRatingOfWest = new BigDecimal(0);
//        private BigDecimal avgRatingOfEast = new BigDecimal(0);
//        private BigDecimal couRatingOfEast = new BigDecimal(0);
//
//        private BigDecimal avgRatingOfSouth = new BigDecimal(0);
//        private BigDecimal couRatingOfSouth = new BigDecimal(0);
//        private BigDecimal avgRatingOfNorth = new BigDecimal(0);
//        private BigDecimal couRatingOfNorth = new BigDecimal(0);
//
//        private BigDecimal avgRatingOfHighAltitude = new BigDecimal(0);
//        private BigDecimal couRatingOfHighAltitude = new BigDecimal(0);
//        private BigDecimal avgRatingOfLowAltitude = new BigDecimal(0);
//        private BigDecimal couRatingOfLowAltitude = new BigDecimal(0);

    public static class Mapper1 extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text>{

        @Override
        public void map(LongWritable longWritable, Text text, OutputCollector<Text, Text> outputCollector, Reporter reporter) throws IOException {

            //split record into attributes with |
            String line = text.toString();
            String[] attris = line.split("\\|");

            //filter out records with illegal longitude or latitude
            double longitude = Double.parseDouble(attris[1]);
            double latitude = Double.parseDouble(attris[2]);
            if(longitude < 8.1461259 || longitude > 11.1993265 || latitude < 56.5824856 || latitude > 57.750511){
                return;
            }

            //format date to "YYYY-MM-DD"
            String reviewDate = formatDate(attris[4]);
            attris[4] = reviewDate;
            String birthDate = formatDate(attris[8]);
            attris[8] = birthDate;

            //format temperature to degree Celsius
            attris[5] = formatTemp(attris[5]);


            //record the max and min of rating for mean normalization in the next round of MapReduce
            if( !attris[6].equals("?"))
            {
                Double rating = Double.parseDouble(attris[6]);
                if (rating > maxRating) {
                    maxRating = rating;
//                    System.out.println("\n\n\n");
//                    System.out.println("\t\t\tmaxRating: " + maxRating);
//                    System.out.println("\t\t\tminRating: " + minRating);
                }
                if (rating < minRating) {
                    minRating = rating;
//                    System.out.println("\n\n\n");
//                    System.out.println("\t\t\tmaxRating: " + maxRating);
//                    System.out.println("\t\t\tminRating: " + minRating);
                }

                /*the rating exists, record  it according to user_income, longitude & latitude*/
                String income = attris[11];
                String longitudeString = attris[1];
                String latitudeString = attris[2];
                String altitudeString = attris[3];
                calRatingDependence(attris[6], income, longitudeString, latitudeString, altitudeString);
            }


            //record the average user_income(if exists) according to nationality & career
            if( !attris[11].equals("?"))
            {
                String income = attris[11];
                String nationality = attris[9];
                String career = attris[10];

                calDependence(income, nationality, avgNation);
                calDependence(income, career, avgCareer);
            }

            //create formatted record
            StringBuilder formattedRecord = new StringBuilder(attris[0]);
            for(int i = 1;  i < attris.length; i++)
            {
                formattedRecord.append("|").append(attris[i]);
            }
            String formattedRecordString = formattedRecord.toString();

            //mapper output format: <user_career, the whole record>
            Text user_career = new Text(attris[10]);
            outputCollector.collect(user_career, new Text(formattedRecordString));
        }

        /**
         * NewInput belongs to an attribute "A".
         * Dependence belongs to another attribute "D".
         * we need to calculate the average "A" of records with the same "D".
         *
         * this function use NewInput to update the average "A" of records with "D" equals to Dependence
         *
         * @param NewInput a new value of "A" from a record with "D" equals to Dependence
         * @param Dependence "D" value of that record
         * @param Avg a Map<K,V> whose [K] is the value of "D", [V] record the average "A" of records with "D" equals to [K]
         */
        private void calDependence(String NewInput, String Dependence, Map<String, Average> Avg) {
            //Avg has not record average "A" of records with "D" equals to Dependence, put into map
            if( !Avg.containsKey(Dependence))
            {
                Average average = new Average(NewInput, "1");
                Avg.put(Dependence, average);
//                System.out.println("get a new nation: " + nationalityString + ", income: " + Avg.get(nationalityString).getAvg());
            }
            //Dependence already in Avg, update average value
            else {
                Average average = Avg.get(Dependence);
                BigDecimal input = new BigDecimal(NewInput);
                average.calculateAverage(input, 0);

//                System.out.println();
//                for(String iterDValue : Avg.keySet())
//                {
//                    BigDecimal value = Avg.get(iterDValue).getAvg();
//                    System.out.println("\t" + iterDValue + "\t" + value);
//                }
            }
        }


        /**
         * calculate average value of ratingString group by incomeString, longitudeString, latitudeString & altitudeString
         *
         * @param ratingString rating value of current record
         * @param incomeString income value of current record
         * @param longitudeString longitude value of current record
         * @param latitudeString latitude value of current record
         * @param altitudeString altitude value of current record
         */
        private void calRatingDependence(String ratingString, String incomeString, String longitudeString, String latitudeString, String altitudeString) {

            BigDecimal rating = new BigDecimal(ratingString);
            //calculate the average rating of high income records and low income records
            if( !incomeString.equals("?"))
            {
                //3247 is the average of the max and the min of income of all records, which is already been calculated, read the report for more
                //low income record
                if (Integer.valueOf(incomeString) <= incomeDivision) {
                    Average average = ratingAvgMap.get("lowIn");
                    average.calculateAverage(rating, 2);

//                    System.out.println();
//                    for(String iterAttri : this.ratingAvgMap.keySet())
//                    {
//                        System.out.println(iterAttri + "\t" + this.ratingAvgMap.get(iterAttri).getAvg());
//                    }
                }
                //high income record
                else {
                    Average average = ratingAvgMap.get("highIn");
                    average.calculateAverage(rating, 2);

//                    System.out.println();
//                    for(String iterAttri : this.ratingAvgMap.keySet())
//                    {
//                        System.out.println(iterAttri + "\t" + this.ratingAvgMap.get(iterAttri).getAvg());
//                    }
                }
            }


            //calculate the average rating of west area and east area according to longitude
            //9.6727262 is the mid of legal longitude range
            //west area
            if (Double.parseDouble(longitudeString) <= longitudeDivision) {
                Average average = ratingAvgMap.get("west");
                average.calculateAverage(rating, 2);

//                System.out.println();
//                for(String iterAttri : this.ratingAvgMap.keySet())
//                {
//                    System.out.println(iterAttri + "\t" + this.ratingAvgMap.get(iterAttri).getAvg());
//                }
            }
            //east area
            else {
                Average average = ratingAvgMap.get("east");
                average.calculateAverage(rating, 2);

//                System.out.println();
//                for(String iterAttri : this.ratingAvgMap.keySet())
//                {
//                    System.out.println(iterAttri + "\t" + this.ratingAvgMap.get(iterAttri).getAvg());
//                }
            }

            //calculate the average rating of south area and north area
            //57.1664983 is the mid of legal latitude range
            //south area
            if(Double.parseDouble(latitudeString) <= latitudeDivision){
                Average average = ratingAvgMap.get("south");
                average.calculateAverage(rating, 2);

//                System.out.println();
//                for(String iterAttri : this.ratingAvgMap.keySet())
//                {
//                    System.out.println(iterAttri + "\t" + this.ratingAvgMap.get(iterAttri).getAvg());
//                }
            }
            //north area
            else {
                Average average = ratingAvgMap.get("north");
                average.calculateAverage(rating, 2);

//                System.out.println();
//                for(String iterAttri : this.ratingAvgMap.keySet())
//                {
//                    System.out.println(iterAttri + "\t" + this.ratingAvgMap.get(iterAttri).getAvg());
//                }
            }


            //calculate the average rating of high altitude area and low altitude area
            //low altitude area
            if(Double.parseDouble(altitudeString) <= altitudeDivision){
                Average average = ratingAvgMap.get("lowAlt");
                average.calculateAverage(rating, 2);

//                System.out.println();
//                for(String iterAttri : this.ratingAvgMap.keySet())
//                {
//                    System.out.println(iterAttri + "\t" + this.ratingAvgMap.get(iterAttri).getAvg());
//                }
            }
            //high altitude area
            else {
                Average average = ratingAvgMap.get("highAlt");
                average.calculateAverage(rating, 2);

//                System.out.println();
//                for(String iterAttri : this.ratingAvgMap.keySet())
//                {
//                    System.out.println(iterAttri + "\t" + this.ratingAvgMap.get(iterAttri).getAvg());
//                }
            }
        }



//        /**
//         * calculate avg by adding current
//         *
//         * DO NOT forget to update cou after calling this function if you need to.
//         *
//         * @param current rating of current record
//         * @param avg average of rating of some records
//         * @param cou amount of some records
//         * @return new average after adding current
//         */
//        private BigDecimal calculateAverage(BigDecimal current, BigDecimal avg, BigDecimal cou)
//        {
//            BigDecimal total = avg.multiply(cou);
//            total = total.add(current);
//            cou = cou.add(new BigDecimal(1));
//
//            return total.divide(cou, 2, BigDecimal.ROUND_HALF_UP);
//        }



        /**
         * format the input date to "YYYY-MM-DD" formation
         * throw RuntimeException if the input is illegal
         *
         * @param inputDate string of "YYYY-MM-DD" or "YYYY/MM/DD" or "Month Date,Year"
         * @return "YYYY-MM-DD" formation
         */
        private String formatDate(String inputDate)
        {
            //already YYYY-MM-DD format
            String[] parseByDash = inputDate.split("-");
            if(parseByDash.length == 3)
            {
                return inputDate;
            }

            // "YYYY/MM/DD" format
            String[] parseBySlash = inputDate.split("/");
            if(parseBySlash.length == 3)
            {
//                String formattedDate = parseBySlash[0] + "-" + parseBySlash[1] + "-" + parseBySlash[2];
//                System.out.println();
//                System.out.println("    originalDate: " + inputDate);
//                System.out.println("    formattedDate: " + formattedDate);
                return parseBySlash[0] + "-" + parseBySlash[1] + "-" + parseBySlash[2];
            }

            //"Month Date,Year" format
            String[] parseIdontKnowHowToSay = inputDate.split("[ ,]");
            if(parseIdontKnowHowToSay.length == 3)
            {
                String month;
                switch (parseIdontKnowHowToSay[0]){
                    case "January":
                        month = "01";
                        break;
                    case "February":
                        month = "02";
                        break;
                    case "March":
                        month = "03";
                        break;
                    case "April":
                        month = "04";
                        break;
                    case "May":
                        month = "05";
                        break;
                    case "June":
                        month = "06";
                        break;
                    case "July":
                        month = "07";
                        break;
                    case "August":
                        month = "08";
                        break;
                    case "September":
                        month = "09";
                        break;
                    case "October":
                        month = "10";
                        break;
                    case "November":
                        month = "11";
                        break;
                    case "December":
                        month = "12";
                        break;
                    default:
                        throw new RuntimeException("Wrong date input: " + inputDate + "\nInput format need to be \"YYYY-MM-DD\" or \"YYYY/MM/DD\" or \"Month Date,Year\"");

                }
                String date;
                if (parseIdontKnowHowToSay[1].length() == 1) {
                    date = "0" + parseIdontKnowHowToSay[1];
                }
                else {
                    date = parseIdontKnowHowToSay[1];
                }

//                String formattedDate = parseIdontKnowHowToSay[2] + "-" + month + "-" + date;
//                System.out.println();
//                System.out.println("    originalDate: " + inputDate);
//                System.out.println("    formattedDate: " + formattedDate);

                return parseIdontKnowHowToSay[2] + "-" + month + "-" + date;
            }

            throw new RuntimeException("Wrong date input: " + inputDate + "\nInput format need to be \"YYYY-MM-DD\" or \"YYYY/MM/DD\" or \"Month Date,Year\"");
        }

        /**
         * convert temperature to degrees Celsius
         * throw RuntimeException if the input is illegal
         *
         * @param temp temperature, need to be "xx.x℃" or "xx.x℉"
         * @return temperature in "xx.x℃" format
         */
        private String formatTemp(String temp)
        {
            if(temp.charAt(temp.length() - 1) == '℃'){
                return temp;
            }
            else if(temp.charAt(temp.length() - 1) == '℉'){
                String[] iniTemp = temp.split("℉");
                if(iniTemp.length != 1){
                    throw new RuntimeException("Wrong temperature input: " + temp);
                }
                double finalTemp = (Double.parseDouble(iniTemp[0]) - 32.0) / 1.8;

                BigDecimal b = new BigDecimal(Double.toString(finalTemp));
                finalTemp = b.setScale(1, BigDecimal.ROUND_HALF_UP).doubleValue();

//                System.out.println();
//                System.out.println("    originalTemp: " + temp);
//                System.out.println("    formattedTemp: " + finalTemp + "℃");

                return finalTemp + "℃";
            }
            else {
                throw new RuntimeException("Wrong temperature input: " + temp);
            }
        }


    }

    public static class Reducer1 extends MapReduceBase implements Reducer<Text, Text, Text, NullWritable>{
        @Override
        public void reduce(Text text, Iterator<Text> records, OutputCollector<Text, NullWritable> outputCollector, Reporter reporter) throws IOException {
            //sample one record from every 100 records with the same key(income)
            int i = 0;
            while(records.hasNext())
            {
                i++;
                Text record = records.next();
                if(i%100 == 1){
                    outputCollector.collect(record, NullWritable.get());
                }
            }
        }
    }

    public static class Mapper2 extends MapReduceBase implements Mapper<LongWritable, Text, Text, NullWritable>{

        @Override
        public void map(LongWritable longWritable, Text text, OutputCollector<Text, NullWritable> outputCollector, Reporter reporter) throws IOException {
            String line = text.toString();
            String[] attris = line.split("\\|");

            String oldRating = attris[6];
            String oldIncome = attris[11];

            //insert lacking income value
            if(oldIncome.equals("?"))
            {
                String nationality = attris[9];
                String career = attris[10];

                BigDecimal avgFromNation = avgNation.get(nationality).getAvg();
                BigDecimal avgFromCareer = avgCareer.get(career).getAvg();

                //get average income from relative attributes
                String newIncome = avgFromCareer.add(avgFromNation)
                        .divide(new BigDecimal("2"), 0, BigDecimal.ROUND_HALF_UP).toString();
                attris[11] = newIncome;
            }

            //insert lacking rating value
            if(oldRating.equals("?"))
            {
                int income = Integer.parseInt(attris[11]);
                double longitude = Double.parseDouble(attris[1]);
                double latitude = Double.parseDouble(attris[2]);
                double altitude = Double.parseDouble(attris[3]);


                BigDecimal avgFromIn;
                BigDecimal avgFromLon;
                BigDecimal avgFromLat;
                BigDecimal avgFromAlt;

                if(income <= incomeDivision)
                {
                    avgFromIn = ratingAvgMap.get("lowIn").getAvg();
                }
                else {
                    avgFromIn = ratingAvgMap.get("highIn").getAvg();
                }

                if(longitude <= longitudeDivision)
                {
                    avgFromLon = ratingAvgMap.get("west").getAvg();
                }
                else {
                    avgFromLon = ratingAvgMap.get("east").getAvg();
                }

                if(latitude <= latitudeDivision)
                {
                    avgFromLat = ratingAvgMap.get("south").getAvg();
                }
                else {
                    avgFromLat = ratingAvgMap.get("north").getAvg();
                }

                if(altitude <= altitudeDivision)
                {
                    avgFromAlt = ratingAvgMap.get("lowAlt").getAvg();
                }
                else {
                    avgFromAlt = ratingAvgMap.get("highAlt").getAvg();
                }

                //get average rating from relative attributes
                String newRating = avgFromIn.add(avgFromLon).add(avgFromLat).add(avgFromAlt)
                        .divide(new BigDecimal("4"), 2, BigDecimal.ROUND_HALF_UP).toString();
                attris[6] = newRating;
            }

            //mean normalize rating value
            double currentRating = Double.parseDouble(attris[6]);
            double normalizedRating = (currentRating - minRating) / (maxRating - minRating);
            attris[6] = new BigDecimal(normalizedRating).setScale(2, BigDecimal.ROUND_HALF_UP).toString();


            //create new record
            StringBuilder formattedRecord = new StringBuilder(attris[0]);
            for(int i = 1;  i < attris.length; i++)
            {
                formattedRecord.append("|").append(attris[i]);
            }
            String formattedRecordString = formattedRecord.toString();

            //mapper output format: <new record, NullWritable>
            outputCollector.collect(new Text(formattedRecordString), NullWritable.get());
        }
    }

    public static class Reducer2 extends MapReduceBase implements Reducer<Text, NullWritable, Text, NullWritable>{

        @Override
        public void reduce(Text text, Iterator<NullWritable> iterator, OutputCollector<Text, NullWritable> outputCollector, Reporter reporter) throws IOException {
            //this reducer need to do nothing  :)
            outputCollector.collect(text, NullWritable.get());
        }
    }

    private static void Round1() throws IOException {

        //set the input and output path in HDFS.
        //change input path to "hdfs://localhost:9000/user/lb/input/Lab1/round1/data.txt" to use the whole data set
        String[] otherArgs=new String[]{
                "hdfs://localhost:9000/user/lb/input/Lab1/round1/data-small.txt",
                "hdfs://localhost:9000/user/lb/output/Lab1/round1"};

        JobConf conf = new JobConf(Lab1.class);
        conf.setJobName("Round1");

        conf.setMapperClass(Mapper1.class);
        conf.setMapOutputKeyClass(Text.class);
        conf.setMapOutputValueClass(Text.class);

        conf.setReducerClass(Reducer1.class);
        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(NullWritable.class);

        conf.setInputFormat(TextInputFormat.class);
        conf.setOutputFormat(TextOutputFormat.class);

        FileInputFormat.setInputPaths(conf,new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(conf, new Path(otherArgs[1]));

        JobClient.runJob(conf);
    }


    private static void Round2() throws IOException {
        String[] otherArgs=new String[]{
                "hdfs://localhost:9000/user/lb/input/Lab1/round2/D_Filter.txt",
                "hdfs://localhost:9000/user/lb/output/Lab1/round2"};

        JobConf conf = new JobConf(Lab1.class);
        conf.setJobName("Round2");

        conf.setMapperClass(Mapper2.class);
        conf.setMapOutputKeyClass(Text.class);
        conf.setMapOutputValueClass(NullWritable.class);

        conf.setReducerClass(Reducer2.class);
        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(NullWritable.class);

        conf.setInputFormat(TextInputFormat.class);
        conf.setOutputFormat(TextOutputFormat.class);

        FileInputFormat.setInputPaths(conf,new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(conf, new Path(otherArgs[1]));

        JobClient.runJob(conf);
    }

    public static void main(String[] args) throws Exception{
        /*
        first round of MapReduce, filter out illegal record, then format record and count some valuable information
        for 2nd round of MapReduce.
         */
        Round1();

        /*
        second round of MapReduce, insert lacking values of rating & user_income.
         */
        Round2();


        //print some important values in Round1
        System.out.println();
        System.out.println();
        System.out.println();
        System.out.println();
        System.out.println();
        System.out.println("MapReduce summarise:");
        System.out.println();
        System.out.println("maxRating: " + maxRating);
        System.out.println("minRating: " + minRating);

        System.out.println();
        for(String iterDValue : avgNation.keySet())
        {
            BigDecimal value = avgNation.get(iterDValue).getAvg();
            System.out.println(iterDValue + "\t" + value);
        }

        System.out.println();
        for(String iterDValue : avgCareer.keySet())
        {
            BigDecimal value = avgCareer.get(iterDValue).getAvg();
            System.out.println(iterDValue + "\t" + value);
        }

        System.out.println();
        for(String iterAttri : ratingAvgMap.keySet())
        {
            System.out.println(iterAttri + "\t" + ratingAvgMap.get(iterAttri).getAvg());
        }

    }

}


