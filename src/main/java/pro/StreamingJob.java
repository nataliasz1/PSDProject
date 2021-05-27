/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package pro;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import static org.apache.avro.SchemaBuilder.array;
import static org.apache.commons.math3.analysis.FunctionUtils.collector;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.ReduceOperator;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.types.DoubleValue;
import org.apache.flink.types.StringValue;
import com.google.common.math.Quantiles;

public class StreamingJob {

    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSet<Tuple6<Double, Double, Double, Double, Double, Double>> data = env.readCsvFile("probki.csv")
                .parseQuotedStrings('"')
                .ignoreInvalidLines()
                .fieldDelimiter(";")
                .types(Double.class, Double.class, Double.class, Double.class, Double.class, Double.class);

        List<Tuple6<Double, Double, Double, Double, Double, Double>> avg = data.
                reduceGroup(new GroupReduceFunction<Tuple6<Double, Double, Double, Double, Double, Double>, Tuple6<Double, Double, Double, Double, Double, Double>>() {
                    @Override
                    public void reduce(Iterable<Tuple6<Double, Double, Double, Double, Double, Double>> iterable, Collector<Tuple6<Double, Double, Double, Double, Double, Double>> collector) throws Exception {
                        int count = 0;
                        double totalScore1 = 0;
                        double totalScore2 = 0;
                        double totalScore3 = 0;
                        double totalScore4 = 0;
                        double totalScore5 = 0;
                        double totalScore6 = 0;
                        for (Tuple6<Double, Double, Double, Double, Double, Double> movie : iterable) {

                            totalScore1 += movie.f0;
                            totalScore2 += movie.f1;
                            totalScore3 += movie.f2;
                            totalScore4 += movie.f3;
                            totalScore5 += movie.f4;
                            totalScore6 += movie.f5;
                            count++;

                        }
                        collector.collect(new Tuple6<>(totalScore1 / count, totalScore2 / count, totalScore3 / count, totalScore4 / count, totalScore5 / count, totalScore6 / count));
                    }
                })
                .collect();

        System.out.println("SREDNIA");
        System.out.println(avg);

        List<Tuple6<Double, Double, Double, Double, Double, Double>> median = data.
                reduceGroup(new GroupReduceFunction<Tuple6<Double, Double, Double, Double, Double, Double>, Tuple6<Double, Double, Double, Double, Double, Double>>() {
                    @Override
                    public void reduce(Iterable<Tuple6<Double, Double, Double, Double, Double, Double>> iterable, Collector<Tuple6<Double, Double, Double, Double, Double, Double>> collector) throws Exception {
                        int count = 0;
                        ArrayList<Double> myList1 = new ArrayList<>();
                        ArrayList<Double> myList2 = new ArrayList<Double>();
                        ArrayList<Double> myList3 = new ArrayList<Double>();
                        ArrayList<Double> myList4 = new ArrayList<Double>();
                        ArrayList<Double> myList5 = new ArrayList<Double>();
                        ArrayList<Double> myList6 = new ArrayList<Double>();
                        for (Tuple6<Double, Double, Double, Double, Double, Double> number : iterable) {

                            myList1.add(number.f0);
                            myList2.add(number.f1);
                            myList3.add(number.f2);
                            myList4.add(number.f3);
                            myList5.add(number.f4);
                            myList6.add(number.f5);

                            count++;

                        }
                        Collections.sort(myList1);
                        Collections.sort(myList2);
                        Collections.sort(myList3);
                        Collections.sort(myList4);
                        Collections.sort(myList5);
                        Collections.sort(myList6);

                        ArrayList<ArrayList<Double>> listOLists = new ArrayList<ArrayList<Double>>();
                        listOLists.add(myList1);
                        listOLists.add(myList2);
                        listOLists.add(myList3);
                        listOLists.add(myList4);
                        listOLists.add(myList5);
                        listOLists.add(myList5);
                        
                        ArrayList<Double> medians = new ArrayList<>();
                        int i =0;
                        for (ArrayList<Double> list : listOLists) {
                            double median;
                            
                            median = Quantiles.median().compute(list);

//                            if (list.size() % 2 == 0) {
//                                median = ((double) list.get(list.size() / 2) + (double) list.get(list.size() / 2 - 1)) / 2;
//                            } else {
//                                median = (double) list.get(list.size() / 2);
//                            }
                            i++;
                            medians.add(median);
                        }

                        collector.collect(new Tuple6<>(medians.get(0), medians.get(1), medians.get(2), medians.get(3), medians.get(4), medians.get(5)));
                    }
                })
                .collect();

        System.out.println("MEDIANA");
        System.out.println(median);      
        
        
        List<Tuple6<Double, Double, Double, Double, Double, Double>> quantile = data.
                reduceGroup(new GroupReduceFunction<Tuple6<Double, Double, Double, Double, Double, Double>, Tuple6<Double, Double, Double, Double, Double, Double>>() {
                    @Override
                    public void reduce(Iterable<Tuple6<Double, Double, Double, Double, Double, Double>> iterable, Collector<Tuple6<Double, Double, Double, Double, Double, Double>> collector) throws Exception {
                        int count = 0;
                        ArrayList<Double> myList1 = new ArrayList<>();
                        ArrayList<Double> myList2 = new ArrayList<Double>();
                        ArrayList<Double> myList3 = new ArrayList<Double>();
                        ArrayList<Double> myList4 = new ArrayList<Double>();
                        ArrayList<Double> myList5 = new ArrayList<Double>();
                        ArrayList<Double> myList6 = new ArrayList<Double>();
                        for (Tuple6<Double, Double, Double, Double, Double, Double> number : iterable) {

                            myList1.add(number.f0);
                            myList2.add(number.f1);
                            myList3.add(number.f2);
                            myList4.add(number.f3);
                            myList5.add(number.f4);
                            myList6.add(number.f5);

                            count++;

                        }
                        

                        ArrayList<ArrayList<Double>> listOLists = new ArrayList<ArrayList<Double>>();
                        listOLists.add(myList1);
                        listOLists.add(myList2);
                        listOLists.add(myList3);
                        listOLists.add(myList4);
                        listOLists.add(myList5);
                        listOLists.add(myList5);
                        
                        ArrayList<Double> quantiles = new ArrayList<>();
                        int i =0;
                        for (ArrayList<Double> list : listOLists) {
                            
                            double myPercentile = Quantiles.percentiles().index(1).compute(list);
                            i++;
                            quantiles.add(myPercentile);
                        }

                        collector.collect(new Tuple6<>(quantiles.get(0), quantiles.get(1), quantiles.get(2), quantiles.get(3), quantiles.get(4), quantiles.get(5)));
                    }
                })
                .collect();

        System.out.println("KWANTYL 0.1");
        System.out.println(quantile); 
        
    }

}
