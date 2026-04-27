/*
 * Copyright (C) 2026 Huawei Technologies Co.,Ltd.
 *
 * dstore is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License as published by
 * the Free Software Foundation; either version 2 of the License, or
 * (at your option) any later version.
 *
 * dstore is distributed in the hope that it will be useful, but
 * WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program. if not, see <https://www.gnu.org/licenses/>.
 */
#include <unistd.h>
#include <getopt.h>

#include "tool_page_dump_file_reader.h"
#include "tool_undo_dump.h"

using namespace DSTORE;
using namespace PageDiagnose;

static const char *PROC_NAME = "pagedump";

static void MemFree(void *ptr)
{
    if (ptr != nullptr) {
        free(ptr);
    }
}

static void PageDumpHelp()
{
    printf(R"(
Usage:
    pagedump options:
    -f, --file=fileId/fileName      target data file:
                                      which needs file name in local disk, when needs file id in pagestore
    -t, --vfs_type=vfsType          value of 0 indicates local disk, when 1 indicates pagestore
    -g, --vfs_config=vfsConfigFile  pagestore config file with absolute address
    -b, --block=blockNum            target relation block number
    -c, --control_file              whether the target file is a control file
    -l, --decode_dict               whether the target file is a decode dict file
    -d, --dis_tuple                 whether to display tuple data for heap page
    -i, --comm_local_ip=localIp     On vfs_type = PageStore, use this param to specify the communication local ip
    -a, --comm_auth_type=VAL        On vfs_type = PageStore, use this param to specify the communication auth type
    -R, --comm_trd=trdMin:trdMax    use this param to specify the communication thread min and max num
    -u, --undo=undocmdid            undo page dump, undocmdid is the undo cmd id: 1 undo map, 2 undo record
                                    -u 1
                                    -u 2 -f [fileid] -b [blocknum] -o [offset]
    -h, --help                      show this help and exit

    tip:
        Additional need to manually add local network ip to the communicationConfig of vfsConfigFile
            with format: "local":{"localIp":"<local network ip>"}, e.g. 127.0.0.1,
            when used to pagestore in crmm node with deploy type of HAS_TENANT_HAS_PAGESTORE,
            and this additional manual action is not required to pagestore in cms node.
)");
}

static RetStatus ParseCmdLineInput(int argc, char **argv, PageDumpConfig *config)
{
    int opt = 0;
    int optIndex = 0;
    const char *shortOptions = "t:g:f:b:cldhi:a:R:u:o:";
    struct option longOptions[] = {
        {"vfs_type", required_argument, nullptr, 't'},
        {"vfs_config", required_argument, nullptr, 'g'},
        {"file", required_argument, nullptr, 'f'},
        {"block", required_argument, nullptr, 'b'},
        {"control_file", no_argument, nullptr, 'c'},
        {"decode_dict", no_argument, nullptr, 'l'},
        {"dis_tuple", no_argument, nullptr, 'd'},
        {"comm_local_ip", required_argument, nullptr, 'i'},
        {"comm_auth_type", required_argument, nullptr, 'a'},
        {"comm_trd", required_argument, nullptr, 'R'},
        {"undo", required_argument, nullptr, 'u'},
        {"offset", required_argument, nullptr, 'o'},
        {"help", no_argument, nullptr, 'h'},
        {nullptr, 0, nullptr, 0}
    };

    while ((opt = getopt_long(argc, argv, shortOptions, longOptions, &optIndex)) != -1) {
        switch (opt) {
            case 't':
                config->vfsType = static_cast<VFSType>(strtol(optarg, nullptr, 0));
                break;
            case 'g':
                config->vfsConfigFile = strdup(optarg);
                break;
            case 'f': {
                config->file = strdup(optarg);
                break;
            }
            case 'b': {
                config->blockNum = static_cast<uint32_t>(strtol(optarg, nullptr, 0));
                break;
            }
            case 'c': {
                config->isControlFile = true;
                break;
            }
            case 'l': {
                config->isDecodeDict = true;
                break;
            }
            case 'd': {
                config->isShowTupleData = true;
                break;
            }
            case 'a':
                if (sscanf_s(optarg, "%u", &config->commConfig.authType) != 1) {
                    (void)fprintf(stderr, "%s: could not parse communication auth type \"%s\"\n", PROC_NAME, optarg);
                    return DSTORE_FAIL;
                }
                break;
            case 'i':
                config->commConfig.localIp = strdup(optarg);
                break;
            case 'R':
                if (STORAGE_FUNC_FAIL(PageDiagnose::ParseCommThreadNum(PROC_NAME, optarg, &config->commConfig))) {
                    return DSTORE_FAIL;
                }
                break;
            case 'u': 
                if (sscanf_s(optarg, "%u", &config->undoCmdId) != 1) {
                    (void)fprintf(stderr, "%s: could not parse undo cmd id \"%s\"\n", PROC_NAME, optarg);
                    return DSTORE_FAIL;
                }
                if (config->undoCmdId != CMD_UNDO_MAP && config->undoCmdId != CMD_UNDO_RECORD) {
                    (void)fprintf(stderr, "Invalid undo cmd id, please try \"%s -h\" for more usage information.\n",
                                  PROC_NAME);
                    return DSTORE_FAIL;
                }
                if (config->undoCmdId == CMD_UNDO_MAP) {
                    config->blockNum = 1;
                    config->isControlFile = true;
                }
                break;
            case 'o':
                config->offset = static_cast<uint32_t>(strtol(optarg, nullptr, 0));
                break;
            case 'h':
            default: {
                PageDumpHelp();
                return DSTORE::DSTORE_FAIL;
            }
        }
    }

    return DSTORE::DSTORE_SUCC;
}

static bool CheckParams(PageDumpConfig config)
{
    if (config.vfsType == VFS_TYPE_PAGE_STORE && config.vfsConfigFile == nullptr) {
        (void)printf("Please input a pagestore config file.\n");
        return false;
    }

    if ((config.vfsType == VFS_TYPE_LOCAL_FS || (!config.isControlFile && !config.isDecodeDict)) &&
        config.file == nullptr && config.undoCmdId == 0) {
        (void)printf("Invalid data file, please try \"%s -h\" for more usage information.\n", PROC_NAME);
        return false;
    }

    if (config.undoCmdId > CMD_UNDO_RECORD ||
        (config.undoCmdId == CMD_UNDO_RECORD &&
         ((config.vfsFileId == 0 && config.file == nullptr) || config.blockNum == 0 || config.offset == 0))) {
        (void)printf("Invalid undo cmd id, please try \"%s -h\" for more usage information.\n", PROC_NAME);
        return false;
    }

    return true;
}

int main(int argc, char **argv)
{
    PageDumpConfig config;
    config.file = nullptr;
    config.blockNum = 0;
    config.isShowTupleData = false;
    config.isControlFile = false;
    config.isDecodeDict = false;
    config.vfsType = VFS_TYPE_LOCAL_FS;
    config.vfsConfigFile = nullptr;
    config.vfsFileId = 0;
    config.undoCmdId = 0;
    PageDiagnose::InitCommConfig(&config.commConfig);

    if (DumpToolHelper::dumpPrint == nullptr) {
        DumpToolHelper::SetPrintTarget(stdout);
    }

    if (ParseCmdLineInput(argc, argv, &config) == DSTORE::DSTORE_FAIL) {
        MemFree(config.vfsConfigFile);
        MemFree(config.file);
        return 0;
    }

    if (!CheckParams(config)) {
        MemFree(config.vfsConfigFile);
        MemFree(config.file);
        return -1;
    }

    if ((!config.isControlFile || config.undoCmdId == 2)) {
        config.vfsFileId = static_cast<uint16_t>(strtol(config.file, nullptr, 0));
    }

    /* remove it later */
    if (STORAGE_FUNC_FAIL(CreateMemoryContextForTool("pagedump memcontext"))) {
        MemFree(config.vfsConfigFile);
        MemFree(config.file);
        return -1;
    }
    if (config.undoCmdId != 0) {
        UndoDumpFileReader undoDump(&config);
        undoDump.Dump();
    } else {
        PageDumpFileReader *pageDump = new PageDumpFileReader(&config);
        if (unlikely(pageDump->OpenFile() == DSTORE::DSTORE_FAIL)) {
            MemFree(config.vfsConfigFile);
            MemFree(config.file);
            delete pageDump;
            return -1;
        }

        /* User request dump which page */
        if (config.blockNum > 0) {
            pageDump->DumpPage(config.blockNum);
        } else {
            pageDump->DumpAllPages();
        }
        delete pageDump;
    }

    MemFree(config.vfsConfigFile);
    MemFree(config.file);
    
    return 0;
}
