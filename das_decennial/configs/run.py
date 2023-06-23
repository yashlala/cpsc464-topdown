import pandas as pd

#Save application ID
df=pd.read_csv("experiments.csv")
app_ID=df.ApplicationID
print(app_ID)

#f = open("run.ini", "r")
with open("run.ini", "r") as f:
    contents=f.readlines()
del contents[218]
contents.insert(218, "\n")
#Add raw and raw housing
contents.insert(218, "keep_attrs = geocode, syn, unit_syn, _invar, _cons, raw, raw_housing")
del contents[77:84]
contents.insert(77, "\n")
# remove spacing between lines for AIAN areas
contents.insert(77, "aian_areas = Legal_Federally_Recognized_American_Indian_Area,American_Indian_Joint_Use_Area,Hawaiian_Home_Land,Alaska_Native_Village_Statistical_Area,State_Recognized_Legal_American_Indian_Area,Oklahoma_Tribal_Statistical_Area,Joint_Use_Oklahoma_Tribal_Statistical_Area")


for i in app_ID:

    i=str(i)
    if (i != "nan"):
        #Create new config file for each application ID
        with open(f"run_copy_{i}.ini", "w") as g:
            #write each new config
            #del contents[29]
            #contents.insert(29, "saved_optimized_app_id:")
            contents = "".join(contents)
            g.write(contents+"\n")
        with open(f"run_copy_{i}.ini", "r") as k:
            #read unique ID
            contents2=k.readlines()
            #contents2.insert(29, "\n")
            contents2.insert(29, f"saved_optimized_app_id:{i}\n")
            #contents2.insert(30, "\n")
            contents2.insert(30, f"saved_noisy_app_id:{i}\n")
            contents2.insert(31, f"optimization_start_from_level: Block")
            contents2.insert(32, f"postprocess_only = on")
        with open(f"run_copy_{i}.ini", "w") as l:
            #write unique ID
            contents2 = "".join(contents2)
            l.write(contents2+"\n")
        with open(f"run_copy_{i}.ini", "r") as j:
            print(j.read())
