
from options import OptManager, OptType, opt_generator

opt_mngr = OptManager()
opt_mngr.register_opt("sample", OptType.YMD_DATE, True)
opt_mngr.register_opt("sample1", OptType.BOOL, False)

print(opt_mngr.usage())

opt_string = "sample=1970-01-01"

opt_dict = dict()
for opt_name, opt_value in opt_generator(opt_string):
    opt_dict[opt_name] = opt_value

print(opt_dict)

processed_opt = opt_mngr.process_dict(opt_dict)

print(processed_opt)
