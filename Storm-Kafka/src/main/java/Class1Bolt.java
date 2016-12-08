import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.ProtocolException;
import java.net.URL;

public class Class1Bolt extends BaseBasicBolt {
    private static final Logger LOG = LoggerFactory.getLogger(Class1Bolt.class);
    public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector) {
        try {
            String s = tuple.getString(0);
            String r[] = s.split("_");
            String filename = r[0];
            String features = r[1];

            double[] feature = fromString(r[1]);
            Boolean check = checkClass1(feature);
            insertIntoMongoDB(check);
            basicOutputCollector.emit(new Values(filename,check));
        }
        catch (Exception e)
        {
            e.printStackTrace();
        }

    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("context","status"));
    }

    private static double[] fromString(String string) {
        String[] strings = string.split(";");
        double result[] = new double[strings.length];
        for (int i = 0; i < result.length; i++) {
            result[i] = Double.parseDouble(strings[i]);
        }
        return result;
    }

    public static void insertIntoMongoDB(Boolean check) {
        String API_KEY = "j1ttIfJ4D9Aol5Ru3bFCtLTAilPI2-v1";
        String DATABASE_NAME = "cs5543";
        String COLLECTION_NAME = "output";
        String urlString = "https://api.mlab.com/api/1/databases/" +
                DATABASE_NAME + "/collections/" + COLLECTION_NAME + "?apiKey=" + API_KEY;
        LOG.info(urlString);

        StringBuilder result = null;
        try {
            URL url = new URL(urlString);
            HttpURLConnection urlConnection = (HttpURLConnection) url.openConnection();
            urlConnection.setDoOutput(true);
            urlConnection.setRequestMethod("POST");
            urlConnection.setRequestProperty("Content-Type", "application/json");
            urlConnection.setRequestProperty("Accept", "application/json");
            Writer writer = new BufferedWriter(new OutputStreamWriter(urlConnection.getOutputStream(), "UTF-8"));
            JSONObject jsonObject = new JSONObject();
            jsonObject.put("Context", "Class1");
            jsonObject.put("Decision", check);
            jsonObject.put("Timestamp", System.currentTimeMillis());
            writer.write(jsonObject.toString());
            LOG.info(jsonObject.toString());
            writer.close();
            BufferedReader in = new BufferedReader(
                    new InputStreamReader(urlConnection.getInputStream()));
            String inputLine;
            StringBuffer response = new StringBuffer();

            while ((inputLine = in.readLine()) != null) {
                response.append(inputLine);
            }
            in.close();

        } catch (MalformedURLException e) {
            e.printStackTrace();
        } catch (ProtocolException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }

        System.out.println("Uploaded data to Mongo");

    }

    public Boolean checkClass1(double[] feature) {
        
DecisionTreeModel classifier of depth 19 with 327 nodes
  if (feature[40] <= 21.0) {
   if (feature[52] <= -107.0) {
    if (feature[56] <= -23.0) {
     if (feature[28] <= -56.0) {
      if (feature[77] <= -128.0) {
       if (feature[98] <= -107.0) {
        if (feature[119] <= -28.0) {
         if (feature[1] <= -121.0) {
          if (feature[89] <= -40.0) {
           if (feature[25] <= -116.0) {
            if (feature[9] <= -45.0) {
             if (feature[51] <= -36.0) {
              if (feature[64] <= -2.0) {
               return false;
              }
              else if (feature[64] > -2.0){
               return false;
              }
             }
             else if (feature[51] > -36.0){
              return false;
             }
            }
            else if (feature[9] > -45.0){
             return true;
            }
           }
           else if (feature[25] > -116.0){
            if (feature[23] <= -104.0) {
             if (feature[4] <= -128.0) {
              return false;
             }
             else if (feature[4] > -128.0){
              return true;
             }
            }
            else if (feature[23] > -104.0){
             return false;
            }
           }
          }
          else if (feature[89] > -40.0){
           return false;
          }
         }
         else if (feature[1] > -121.0){
          if (feature[35] <= -119.0) {
           if (feature[38] <= -128.0) {
            if (feature[65] <= -38.0) {
             if (feature[99] <= -126.0) {
              if (feature[12] <= -127.0) {
               if (feature[19] <= -113.0) {
                if (feature[97] <= -70.0) {
                 if (feature[123] <= -35.0) {
                  if (feature[41] <= 3.0) {
                   return false;
                  }
                  else if (feature[41] > 3.0){
                   if (feature[0] <= -89.0) {
                    return false;
                   }
                   else if (feature[0] > -89.0){
                    return false;
                   }
                  }
                 }
                 else if (feature[123] > -35.0){
                  return false;
                 }
                }
                else if (feature[97] > -70.0){
                 return false;
                }
               }
               else if (feature[19] > -113.0){
                if (feature[2] <= -91.0) {
                 return false;
                }
                else if (feature[2] > -91.0){
                 return false;
                }
               }
              }
              else if (feature[12] > -127.0){
               return false;
              }
             }
             else if (feature[99] > -126.0){
              return true;
             }
            }
            else if (feature[65] > -38.0){
             if (feature[64] <= -14.0) {
              return false;
             }
             else if (feature[64] > -14.0){
              return false;
             }
            }
           }
           else if (feature[38] > -128.0){
            if (feature[60] <= -128.0) {
             if (feature[2] <= -91.0) {
              return false;
             }
             else if (feature[2] > -91.0){
              return false;
             }
            }
            else if (feature[60] > -128.0){
             if (feature[14] <= -127.0) {
              return false;
             }
             else if (feature[14] > -127.0){
              return true;
             }
            }
           }
          }
          else if (feature[35] > -119.0){
           if (feature[51] <= -123.0) {
            if (feature[119] <= -104.0) {
             if (feature[35] <= -66.0) {
              return true;
             }
             else if (feature[35] > -66.0){
              return false;
             }
            }
            else if (feature[119] > -104.0){
             return false;
            }
           }
           else if (feature[51] > -123.0){
            if (feature[14] <= -128.0) {
             if (feature[19] <= -47.0) {
              return false;
             }
             else if (feature[19] > -47.0){
              return true;
             }
            }
            else if (feature[14] > -128.0){
             return false;
            }
           }
          }
         }
        }
        else if (feature[119] > -28.0){
         if (feature[14] <= -128.0) {
          if (feature[57] <= -107.0) {
           return true;
          }
          else if (feature[57] > -107.0){
           if (feature[0] <= -128.0) {
            return false;
           }
           else if (feature[0] > -128.0){
            return false;
           }
          }
         }
         else if (feature[14] > -128.0){
          return false;
         }
        }
       }
       else if (feature[98] > -107.0){
        if (feature[91] <= -128.0) {
         return true;
        }
        else if (feature[91] > -128.0){
         return false;
        }
       }
      }
      else if (feature[77] > -128.0){
       if (feature[117] <= -111.0) {
        if (feature[22] <= -124.0) {
         if (feature[51] <= -128.0) {
          if (feature[107] <= -115.0) {
           if (feature[66] <= -107.0) {
            if (feature[19] <= -123.0) {
             if (feature[63] <= -92.0) {
              if (feature[78] <= -14.0) {
               if (feature[76] <= -18.0) {
                if (feature[55] <= -23.0) {
                 if (feature[14] <= -75.0) {
                  if (feature[24] <= -9.0) {
                   if (feature[35] <= -39.0) {
                    return true;
                   }
                   else if (feature[35] > -39.0){
                    if (feature[0] <= -126.0) {
                     return true;
                    }
                    else if (feature[0] > -126.0){
                     return false;
                    }
                   }
                  }
                  else if (feature[24] > -9.0){
                   return false;
                  }
                 }
                 else if (feature[14] > -75.0){
                  return false;
                 }
                }
                else if (feature[55] > -23.0){
                 return false;
                }
               }
               else if (feature[76] > -18.0){
                return false;
               }
              }
              else if (feature[78] > -14.0){
               return false;
              }
             }
             else if (feature[63] > -92.0){
              return false;
             }
            }
            else if (feature[19] > -123.0){
             return false;
            }
           }
           else if (feature[66] > -107.0){
            if (feature[0] <= -124.0) {
             return false;
            }
            else if (feature[0] > -124.0){
             return true;
            }
           }
          }
          else if (feature[107] > -115.0){
           if (feature[0] <= -66.0) {
            return false;
           }
           else if (feature[0] > -66.0){
            return true;
           }
          }
         }
         else if (feature[51] > -128.0){
          if (feature[49] <= -26.0) {
           if (feature[49] <= -92.0) {
            if (feature[17] <= -116.0) {
             return false;
            }
            else if (feature[17] > -116.0){
             return false;
            }
           }
           else if (feature[49] > -92.0){
            if (feature[33] <= -123.0) {
             if (feature[56] <= -92.0) {
              return false;
             }
             else if (feature[56] > -92.0){
              if (feature[8] <= -111.0) {
               return true;
              }
              else if (feature[8] > -111.0){
               return false;
              }
             }
            }
            else if (feature[33] > -123.0){
             if (feature[80] <= -7.0) {
              if (feature[3] <= -125.0) {
               return false;
              }
              else if (feature[3] > -125.0){
               return true;
              }
             }
             else if (feature[80] > -7.0){
              return false;
             }
            }
           }
          }
          else if (feature[49] > -26.0){
           return true;
          }
         }
        }
        else if (feature[22] > -124.0){
         if (feature[81] <= -110.0) {
          if (feature[93] <= -114.0) {
           if (feature[16] <= -68.0) {
            return true;
           }
           else if (feature[16] > -68.0){
            return false;
           }
          }
          else if (feature[93] > -114.0){
           if (feature[21] <= -114.0) {
            return false;
           }
           else if (feature[21] > -114.0){
            return false;
           }
          }
         }
         else if (feature[81] > -110.0){
          if (feature[93] <= -48.0) {
           if (feature[23] <= -128.0) {
            return true;
           }
           else if (feature[23] > -128.0){
            if (feature[83] <= -31.0) {
             if (feature[88] <= -27.0) {
              if (feature[80] <= 22.0) {
               if (feature[47] <= -1.0) {
                if (feature[53] <= -70.0) {
                 if (feature[2] <= -81.0) {
                  if (feature[109] <= -74.0) {
                   return false;
                  }
                  else if (feature[109] > -74.0){
                   if (feature[0] <= -128.0) {
                    return false;
                   }
                   else if (feature[0] > -128.0){
                    return false;
                   }
                  }
                 }
                 else if (feature[2] > -81.0){
                  if (feature[10] <= -92.0) {
                   return false;
                  }
                  else if (feature[10] > -92.0){
                   return false;
                  }
                 }
                }
                else if (feature[53] > -70.0){
                 return false;
                }
               }
               else if (feature[47] > -1.0){
                return false;
               }
              }
              else if (feature[80] > 22.0){
               if (feature[3] <= -124.0) {
                return true;
               }
               else if (feature[3] > -124.0){
                return false;
               }
              }
             }
             else if (feature[88] > -27.0){
              return false;
             }
            }
            else if (feature[83] > -31.0){
             return true;
            }
           }
          }
          else if (feature[93] > -48.0){
           if (feature[0] <= -125.0) {
            return false;
           }
           else if (feature[0] > -125.0){
            if (feature[0] <= -119.0) {
             return true;
            }
            else if (feature[0] > -119.0){
             return false;
            }
           }
          }
         }
        }
       }
       else if (feature[117] > -111.0){
        if (feature[89] <= -40.0) {
         if (feature[48] <= 17.0) {
          if (feature[11] <= -86.0) {
           if (feature[12] <= -92.0) {
            if (feature[106] <= -123.0) {
             if (feature[52] <= -110.0) {
              if (feature[94] <= -34.0) {
               return true;
              }
              else if (feature[94] > -34.0){
               if (feature[0] <= -125.0) {
                return false;
               }
               else if (feature[0] > -125.0){
                return true;
               }
              }
             }
             else if (feature[52] > -110.0){
              return false;
             }
            }
            else if (feature[106] > -123.0){
             if (feature[1] <= -128.0) {
              return false;
             }
             else if (feature[1] > -128.0){
              return false;
             }
            }
           }
           else if (feature[12] > -92.0){
            return false;
           }
          }
          else if (feature[11] > -86.0){
           if (feature[8] <= -99.0) {
            return false;
           }
           else if (feature[8] > -99.0){
            return false;
           }
          }
         }
         else if (feature[48] > 17.0){
          return false;
         }
        }
        else if (feature[89] > -40.0){
         return false;
        }
       }
      }
     }
     else if (feature[28] > -56.0){
      if (feature[18] <= -118.0) {
       if (feature[123] <= 3.0) {
        if (feature[16] <= 29.0) {
         return false;
        }
        else if (feature[16] > 29.0){
         return false;
        }
       }
       else if (feature[123] > 3.0){
        if (feature[3] <= -128.0) {
         return true;
        }
        else if (feature[3] > -128.0){
         return false;
        }
       }
      }
      else if (feature[18] > -118.0){
       if (feature[103] <= -123.0) {
        if (feature[16] <= -9.0) {
         return false;
        }
        else if (feature[16] > -9.0){
         return false;
        }
       }
       else if (feature[103] > -123.0){
        if (feature[1] <= -113.0) {
         return false;
        }
        else if (feature[1] > -113.0){
         return true;
        }
       }
      }
     }
    }
    else if (feature[56] > -23.0){
     if (feature[79] <= -46.0) {
      if (feature[123] <= -35.0) {
       if (feature[88] <= -99.0) {
        return false;
       }
       else if (feature[88] > -99.0){
        if (feature[16] <= 53.0) {
         if (feature[103] <= -41.0) {
          return false;
         }
         else if (feature[103] > -41.0){
          return true;
         }
        }
        else if (feature[16] > 53.0){
         return true;
        }
       }
      }
      else if (feature[123] > -35.0){
       return true;
      }
     }
     else if (feature[79] > -46.0){
      return false;
     }
    }
   }
   else if (feature[52] > -107.0){
    if (feature[123] <= -79.0) {
     if (feature[114] <= -65.0) {
      if (feature[105] <= -31.0) {
       if (feature[108] <= -78.0) {
        if (feature[72] <= 22.0) {
         if (feature[21] <= -25.0) {
          if (feature[104] <= 13.0) {
           if (feature[84] <= -128.0) {
            if (feature[0] <= -128.0) {
             return false;
            }
            else if (feature[0] > -128.0){
             return true;
            }
           }
           else if (feature[84] > -128.0){
            if (feature[67] <= -22.0) {
             if (feature[3] <= -20.0) {
              return false;
             }
             else if (feature[3] > -20.0){
              if (feature[0] <= -126.0) {
               return false;
              }
              else if (feature[0] > -126.0){
               return false;
              }
             }
            }
            else if (feature[67] > -22.0){
             if (feature[3] <= -85.0) {
              return false;
             }
             else if (feature[3] > -85.0){
              return false;
             }
            }
           }
          }
          else if (feature[104] > 13.0){
           return false;
          }
         }
         else if (feature[21] > -25.0){
          if (feature[3] <= -128.0) {
           if (feature[6] <= -114.0) {
            return true;
           }
           else if (feature[6] > -114.0){
            return false;
           }
          }
          else if (feature[3] > -128.0){
           return false;
          }
         }
        }
        else if (feature[72] > 22.0){
         return true;
        }
       }
       else if (feature[108] > -78.0){
        if (feature[9] <= -103.0) {
         if (feature[16] <= -92.0) {
          return false;
         }
         else if (feature[16] > -92.0){
          return false;
         }
        }
        else if (feature[9] > -103.0){
         return true;
        }
       }
      }
      else if (feature[105] > -31.0){
       if (feature[7] <= -60.0) {
        if (feature[80] <= -31.0) {
         return false;
        }
        else if (feature[80] > -31.0){
         return false;
        }
       }
       else if (feature[7] > -60.0){
        return true;
       }
      }
     }
     else if (feature[114] > -65.0){
      if (feature[1] <= -118.0) {
       return true;
      }
      else if (feature[1] > -118.0){
       if (feature[57] <= -121.0) {
        return false;
       }
       else if (feature[57] > -121.0){
        if (feature[3] <= -85.0) {
         return false;
        }
        else if (feature[3] > -85.0){
         return true;
        }
       }
      }
     }
    }
    else if (feature[123] > -79.0){
     if (feature[39] <= -128.0) {
      if (feature[0] <= -128.0) {
       return true;
      }
      else if (feature[0] > -128.0){
       return false;
      }
     }
     else if (feature[39] > -128.0){
      if (feature[31] <= -112.0) {
       return true;
      }
      else if (feature[31] > -112.0){
       return false;
      }
     }
    }
   }
  }
  else if (feature[40] > 21.0){
   if (feature[30] <= -128.0) {
    if (feature[27] <= -123.0) {
     if (feature[88] <= -102.0) {
      if (feature[70] <= -62.0) {
       if (feature[126] <= -122.0) {
        if (feature[17] <= -126.0) {
         if (feature[9] <= -127.0) {
          return true;
         }
         else if (feature[9] > -127.0){
          return false;
         }
        }
        else if (feature[17] > -126.0){
         if (feature[90] <= -120.0) {
          if (feature[48] <= 34.0) {
           if (feature[61] <= -105.0) {
            if (feature[119] <= -99.0) {
             return true;
            }
            else if (feature[119] > -99.0){
             if (feature[1] <= -120.0) {
              return true;
             }
             else if (feature[1] > -120.0){
              return false;
             }
            }
           }
           else if (feature[61] > -105.0){
            if (feature[0] <= -94.0) {
             return false;
            }
            else if (feature[0] > -94.0){
             return true;
            }
           }
          }
          else if (feature[48] > 34.0){
           return false;
          }
         }
         else if (feature[90] > -120.0){
          if (feature[32] <= -84.0) {
           if (feature[0] <= -125.0) {
            return false;
           }
           else if (feature[0] > -125.0){
            return true;
           }
          }
          else if (feature[32] > -84.0){
           return false;
          }
         }
        }
       }
       else if (feature[126] > -122.0){
        if (feature[2] <= -109.0) {
         return false;
        }
        else if (feature[2] > -109.0){
         return true;
        }
       }
      }
      else if (feature[70] > -62.0){
       if (feature[1] <= -120.0) {
        return false;
       }
       else if (feature[1] > -120.0){
        return false;
       }
      }
     }
     else if (feature[88] > -102.0){
      if (feature[119] <= -53.0) {
       if (feature[47] <= -88.0) {
        return false;
       }
       else if (feature[47] > -88.0){
        if (feature[0] <= -89.0) {
         return false;
        }
        else if (feature[0] > -89.0){
         return true;
        }
       }
      }
      else if (feature[119] > -53.0){
       return true;
      }
     }
    }
    else if (feature[27] > -123.0){
     if (feature[114] <= -126.0) {
      if (feature[59] <= -75.0) {
       return false;
      }
      else if (feature[59] > -75.0){
       return false;
      }
     }
     else if (feature[114] > -126.0){
      if (feature[2] <= -120.0) {
       return false;
      }
      else if (feature[2] > -120.0){
       return true;
      }
     }
    }
   }
   else if (feature[30] > -128.0){
    if (feature[88] <= -59.0) {
     if (feature[44] <= -114.0) {
      if (feature[127] <= -50.0) {
       if (feature[91] <= -40.0) {
        if (feature[75] <= -116.0) {
         if (feature[101] <= -56.0) {
          if (feature[41] <= -27.0) {
           return false;
          }
          else if (feature[41] > -27.0){
           if (feature[0] <= -78.0) {
            return false;
           }
           else if (feature[0] > -78.0){
            return true;
           }
          }
         }
         else if (feature[101] > -56.0){
          if (feature[0] <= -94.0) {
           return true;
          }
          else if (feature[0] > -94.0){
           return false;
          }
         }
        }
        else if (feature[75] > -116.0){
         return true;
        }
       }
       else if (feature[91] > -40.0){
        return false;
       }
      }
      else if (feature[127] > -50.0){
       return false;
      }
     }
     else if (feature[44] > -114.0){
      if (feature[12] <= -103.0) {
       return true;
      }
      else if (feature[12] > -103.0){
       return false;
      }
     }
    }
    else if (feature[88] > -59.0){
     if (feature[64] <= -99.0) {
      if (feature[8] <= -27.0) {
       return false;
      }
      else if (feature[8] > -27.0){
       return false;
      }
     }
     else if (feature[64] > -99.0){
      return true;
     }
    }
   }
  }
    return false;
    }
}
