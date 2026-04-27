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
/*

fuzz算法调度的核心文件
对本文件的修改，需要谨慎

*/

#include "PCommon.h"

char* LIB_INFO = "Secodefuzz version: v1.2.1 (2019-08-08, 11:11)";

const struct Mutater_group* g_Mutater_group[enum_MutatedMAX];
int g_IsMutatedClose[enum_MutatedMAX] = {enum_No};

//在各变异算法计算测试用例的时候，必须首先考虑最大尺寸
//这个值不能设置太小，保证只有能够变的很大尺寸的变异算法才考虑这个因素
// 受系统所限不能太多，推荐65535
int maxOutputSize = Default_maxOutputSize;  // 10000000
int g_StringHasTerminal = 1;                //定义输出的变异的字符串是否有/0结尾

//给样本文件参数起名字用的起始数字
static int nameNo = 1;

char* GET_Version()
{
    return LIB_INFO;
}

void DT_Set_MaxOutputSize(int imaxOutputSize)
{
    if (imaxOutputSize > MAX_valueBuf)
        imaxOutputSize = MAX_valueBuf;

    maxOutputSize = imaxOutputSize;
}

void DT_Set_String_Has_Terminal(int is_string_has_terminal)
{
    g_StringHasTerminal = is_string_has_terminal;
}

int register_Mutater(const struct Mutater_group* pMutater_group, enum enum_Mutated MutatedNum)
{
    g_Mutater_group[MutatedNum] = pMutater_group;
    return 1;
}

/*
调用本函数之前，下边变量必须赋值,本函数给有追求的人调用，一般人远离
isNeedFree
isHasInitValue
type
isHasSigned 	数字类型必选
inLen
inBuf
numberValue
好吧，还是不给外人调用了:)
*/

static void get_ElementName(S_Element* pElement)
{
    switch (pElement->para.type) {
        case enum_NumberU:
            hw_sprintf(pElement->para.name, "NumberU-%d", nameNo++);
            break;
        case enum_NumberS:
            hw_sprintf(pElement->para.name, "NumberS-%d", nameNo++);
            break;
        case enum_Number_Enum:
            hw_sprintf(pElement->para.name, "NumberEnum-%d", nameNo++);
            break;
        case enum_Number_Range:
            hw_sprintf(pElement->para.name, "NumberRange-%d", nameNo++);
            break;
        case enum_String:
            hw_sprintf(pElement->para.name, "String-%d", nameNo++);
            break;
        case enum_String_Enum:
            hw_sprintf(pElement->para.name, "StringEnum-%d", nameNo++);
            break;
        case enum_Blob:
            hw_sprintf(pElement->para.name, "Blob-%d", nameNo++);
            break;
        case enum_Blob_Enum:
            hw_sprintf(pElement->para.name, "BlobEnum-%d", nameNo++);
            break;
        case enum_FixBlob:
            hw_sprintf(pElement->para.name, "FixBlob-%d", nameNo++);
            break;
        case enum_AFL:
            hw_sprintf(pElement->para.name, "AFL-%d", nameNo++);
            break;
        case enum_Ipv4:
            hw_sprintf(pElement->para.name, "Ipv4-%d", nameNo++);
            break;
        default:
            hw_sprintf(pElement->para.name, "default-%d", nameNo++);
            break;
    }

    return;
}

static void CREAT_Element(S_Element* pElement)
{
    int i = 0;

    ASSERT_NULL(pElement);
    ASSERT_NEGATIVE(pElement->inLen);
    ASSERT_NEGATIVE(pElement->para.max_len);

    get_ElementName(pElement);

    if (isLogOpen)
        hw_printf("[*-*] Creat Element %s\n", pElement->para.name);

    pElement->count = 0;
    pElement->pos = 0;

    //字符串变量，初始值如果可以转化为数字，则获取s64类型的数字变异算法
    if (in_StringIsNumber(pElement) == enum_Yes) {
        *(s64*)pElement->numberValue = (s64)in_atol(pElement->inBuf);
    }

    //得到一共有多少测试例，支持哪些变异算法
    for (i = 0; i < enum_MutatedMAX; i++) {
        //等于空说明该变异算法没有被注册
        if (g_Mutater_group[i] == NULL)
            continue;

        //看变异算法全局开关是否被关闭
        if (g_IsMutatedClose[i] == enum_Yes)
            continue;

        //看变异算法元素级别开关是否被关闭，这个现在不起作用，需要好好想想
        if (pElement->isMutatedClose[i] == enum_Yes)
            continue;

        //看该元素是否被该变异算法支持
        if (g_Mutater_group[i]->getIsSupport(pElement) == enum_No)
            continue;

        //设置该变异算法被支持，设置该算法一共多少测试例
        pElement->isMutatedSupport[i] = enum_Yes;
        pElement->num[i] = g_Mutater_group[i]->getCount(pElement);

        if (isLogOpen)
            hw_printf(
                "[*] %s Get Count %d Mutator: %s\n", pElement->para.name, pElement->num[i], g_Mutater_group[i]->name);

        pElement->numbak[i] = g_Mutater_group[i]->getCount(pElement);

        //设置每个变异算法起始测视力的位置，共后边计算pos落入到那个变异算法里
        pElement->posStart[i] = pElement->count;

        //设置该元素一共多少测试例
        pElement->count += pElement->num[i];
    }
}

void CREAT_ElementEX(S_Element* pElement, int isNeedFree, int isHasInitValue, int type, char* inBuf, int inLen)
{
    ASSERT_NULL(pElement);

    pElement->isNeedFree = isNeedFree;
    pElement->isHasInitValue = isHasInitValue;
    pElement->para.type = type;
    pElement->inBuf = inBuf;
    pElement->inLen = inLen;

    if (pElement->para.max_len <= 0)
        pElement->para.max_len = maxOutputSize;

    if (pElement->para.max_len > maxOutputSize) {
        pElement->para.max_len = maxOutputSize;
        M_DEBUG(1, "maxLen(%d) is larger than maxOutputSize(%d).\n", pElement->para.max_len, maxOutputSize);
    }

    if (pElement->para.max_len < (pElement->inLen / 8)) {
        M_DEBUG(
            1, "maxLen can not be less than inLen. %d , in len%ld\n", pElement->para.max_len, (pElement->inLen / 8));
        pElement->para.max_len = maxOutputSize;
    }

    CREAT_Element(pElement);
}

void FREE_Element(S_Element* pElement)
{
    if (pElement != NULL) {
        if (pElement->isNeedFree) {
            if (isLogOpen)
                hw_printf("[*-*] Free Element %s\n", pElement->para.name);

            hw_Free(pElement);
            pElement = NULL;
        }
    }
}

void ADD_WholeRandom(S_Element* pElement)
{
    if (pElement->isAddWholeRandom == enum_No) {
        pElement->isAddWholeRandom = enum_Yes;
        addWholeRandomNum++;
    }
}

void LEAVE_WholeRandom(S_Element* pElement)
{
    if (pElement->isAddWholeRandom == enum_Yes) {
        pElement->isAddWholeRandom = enum_No;
        addWholeRandomNum--;
    }
}

void ADD_WholeSequence(S_Element* pElement)
{
    if (pElement->isAddWholeSequence == enum_No) {
        pElement->isAddWholeSequence = enum_Yes;
        addWholeSequenceNum++;
        pElement->sequenceStartPos = tempSequenceStartPos;
        tempSequenceStartPos += pElement->count;
    }
}

void LEAVE_WholeSequence(S_Element* pElement)
{
    if (pElement->isAddWholeSequence == enum_Yes) {
        pElement->isAddWholeSequence = enum_No;
        addWholeSequenceNum--;
    }
}

int GET_WholeSequenceTotalNumber(void)
{
    return tempSequenceStartPos;
}

//获取测试例的统一接口，获取后需自己强制转换成自己想要的类型
//目前限制为内部结构，不对外提供
static char* GET_MutatedValue(S_Element* pElement, int pos)
{
    int i = 0;
    pElement->pos = pos;

    if (isLogOpen)
        hw_printf("[*] %s Performing iteration [%d] \n", pElement->para.name, pos);

    //如果使用原始位置，则用初始值
    if (pos == PosOriginal) {
        set_ElementOriginalValue(pElement);
        hw_Memcpy(pElement->para.mutater_name, "no mutator", in_strlen("no mutator") + 1);

    } else {
        for (i = 0; i < enum_MutatedMAX; i++) {
            //等于空说明该变异算法没有被注册
            if (g_Mutater_group[i] == NULL)
                continue;

            if (pElement->isMutatedSupport[i] == enum_No)
                continue;
            // wait bug2
            if ((pos >= pElement->posStart[i]) && (pos < (pElement->posStart[i] + pElement->num[i]))) {
                if (g_Mutater_group[i]->getValue) {
                    g_Mutater_group[i]->getValue(pElement, pos - pElement->posStart[i]);
                    hw_Memcpy(
                        pElement->para.mutater_name, g_Mutater_group[i]->name, in_strlen(g_Mutater_group[i]->name) + 1);

                    //统一重新申请内存，放置变异数据
                    int len = pElement->para.len;
                    if ((pElement->para.len > 0) || (pElement->para.type == enum_FixBlob)) {
                        if (pElement->para.len > pElement->para.max_len)
                            pElement->para.len = pElement->para.max_len;

                        //固定长度，变异的长度可能会有变化，重置之
                        if (pElement->para.type == enum_FixBlob) {
                            pElement->para.len = pElement->para.max_len;
                        }

                        char* temp1 = hw_Malloc(pElement->para.len);

                        if (pElement->para.type == enum_FixBlob) {
                            hw_Memset(temp1, pElement->para.len, 0);
                        }

                        hw_Memcpy(temp1, pElement->para.value, MIN(len, pElement->para.len));

                        if (pElement->isNeedFreeOutBuf == enum_Yes)
                            if (pElement->para.value != NULL) {
                                hw_Free(pElement->para.value);
                                pElement->para.value = NULL;
                                pElement->isNeedFreeOutBuf = enum_No;
                            }

                        pElement->para.value = temp1;
                        pElement->isNeedFreeOutBuf = enum_Yes;
                    }

                    //字符串统一复制结束符，这个也许会删除，看情况吧
                    if ((g_StringHasTerminal == 1) && (pElement->para.type == enum_String) && (pElement->para.len > 0))
                        pElement->para.value[pElement->para.len - 1] = 0;

                    if (isLogOpen)
                        hw_printf("[*] Mutator: %s\n", g_Mutater_group[i]->name);
                }
                break;
            }
        }
    }

    if (isLogOpen) {
        // int len = (pElement->outLen/8 + IsAddone(pElement->outLen%8));
        u32 len = (u32)(pElement->para.len);
        hw_printf("[*] output size is %d\n", len);

        if (len <= debugmaxOutputSize)
            HEX_Dump((u8*)pElement->para.value, len);
        else {
            HEX_Dump((u8*)pElement->para.value, debugmaxOutputSize);
            hw_printf("[*] totol len is %d , only display %d ... ...\n", len, debugmaxOutputSize);
        }
    }
    return pElement->para.value;
}

int GET_MutatedValueLen(S_Element* pElement)
{
    int length = 0;
    length = pElement->para.len;
    return length;
}

int GET_IsBeMutated(S_Element* pElement)
{
    if (pElement->pos == PosOriginal)
        return 0;
    return 1;
}

char* GET_MutatedValueSequence(S_Element* pElement, int pos)
{
    int tempPos = PosOriginal;

    if (pElement->isAddWholeSequence == enum_Yes) {
        if ((pos >= pElement->sequenceStartPos) && (pos < (pElement->sequenceStartPos + pElement->count)))
            tempPos = pos - pElement->sequenceStartPos;
        else
            tempPos = PosOriginal;
    } else
        tempPos = pos;

    //没有编译算法就用原始值，实际上这是个容错处理，正常不应该到这里来
    if (0 == pElement->count) {
        tempPos = PosOriginal;
    }

    return GET_MutatedValue(pElement, tempPos);
}

char* GET_MutatedValueRandom(S_Element* pElement)
{
    int pos = PosOriginal;

    //目前的设计，只要是加入整体随机，则就有一定概率才能获取变异值，可能需要改变
    if ((pElement->isAddWholeRandom == enum_Yes) && (get_IsMutated() != enum_Yes))
        pos = PosOriginal;
    else {
        //如果没有变异值，则取原始值就ok
        if (pElement->count == 0)
            pos = PosOriginal;
        else {
            pos = RAND_RANGE(0, pElement->count - 1);
        }
    }

    //没有任何变异算法，就每次使用初始值
    if (0 == pElement->count) {
        pos = PosOriginal;
    }

    if (pos >= pElement->count) {
        M_DEBUG(1, "\tCount of element is greater than pos!\n");
        return NULL;
        // pos = pos % (pElement->count + 1);
    }

    return GET_MutatedValue(pElement, pos);
}

//该函数仅仅free临时内存，每次获取测试例后都要调用
void FREE_MutatedValue(S_Element* pElement)
{

    if (pElement->isNeedFreeOutBuf)
        if (pElement->para.value != NULL) {
            hw_Free(pElement->para.value);
            pElement->para.value = NULL;
            pElement->isNeedFreeOutBuf = enum_No;
        }
}

//下边函数操作全局变异算法开关，对外提供
void SET_CloseAllMutater(void)
{
    int i;
    for (i = 0; i < enum_MutatedMAX; i++) {
        g_IsMutatedClose[i] = enum_Yes;
    }
}

void SET_CloseOneMutater(enum enum_Mutated MutatedNum)
{
    g_IsMutatedClose[MutatedNum] = enum_Yes;
}

void SET_OpenAllMutater(void)
{
    int i;
    for (i = 0; i < enum_MutatedMAX; i++) {
        g_IsMutatedClose[i] = enum_No;
    }
}

void SET_OpenOneMutater(enum enum_Mutated MutatedNum)
{
    g_IsMutatedClose[MutatedNum] = enum_No;
}

//下边函数操作单独元素变异算法开关，对外提供
void SET_ElementCloseAllMutater(S_Element* pElement)
{
    int i;
    for (i = 0; i < enum_MutatedMAX; i++) {
        pElement->isMutatedClose[i] = enum_Yes;
    }
}

void SET_ElementCloseOneMutater(S_Element* pElement, enum enum_Mutated MutatedNum)
{
    pElement->isMutatedClose[MutatedNum] = enum_Yes;
}

void SET_ElementOpenAllMutater(S_Element* pElement)
{
    int i;
    for (i = 0; i < enum_MutatedMAX; i++) {
        pElement->isMutatedClose[i] = enum_No;
    }
}

void SET_ElementOpenOneMutater(S_Element* pElement, enum enum_Mutated MutatedNum)
{
    pElement->isMutatedClose[MutatedNum] = enum_No;
}

//本模块初始化函数，必须在最开始调用
//干掉某个变异算法的一种方法是，直接注释掉初始化函数的调用
void INIT_Common(void)
{

    //初始化所有变异算法结构体
    hw_Memset((void*)g_Mutater_group, 0, sizeof(g_Mutater_group));

    //初始化变异算法，如果某变异算法初始化没有被调用，则该变异算法不起作用
    init_BlobChangeBinaryInteger();
    init_BlobChangeFromNull();
    init_BlobChangeRandom();
    init_BlobChangeSpecial();
    init_BlobChangeToNull();
    init_BlobExpandAllRandom();
    init_BlobExpandSingleIncrementing();
    init_BlobExpandSingleRandom();
    init_BlobExpandZero();
    init_BlobMagic();
    init_BlobEnum();

    //这三为定制变异算法
    init_CustomNumber();
    init_CustomBlob();
    init_CustomString();

    init_DataElementReduce();
    init_DataElementDuplicate();
    init_DataElementBitFlipper();
    init_DataElementBitFill();
    init_DataElementBitZero();
    init_DataElementChangeASCIIInteger();
    init_DataElementMBitFlipper();
    init_DataElementLengthEdgeCase();
    init_DataElementLengthRandom();
    init_DataElementLengthGauss();
    init_DataElementLengthRepeatPart();
    init_DataElementByteRandom();
    init_DataElementOneByteInsert();
    init_DataElementSwapTwoPart();

    init_DataElementStringStatic();
    init_DataElementCopyPartOf();
    init_DataElementInsertPartOf();
    init_DataElementAFL();
    init_DataElementMagic();

    init_NumberEdgeCase();
    init_NumberEdgeRange();
    init_NumberRandom();
    init_NumberVariance();
    init_NumberSmallRange();
    init_NumberPowerRandom();
    init_NumberMagic();
    init_NumberEnum();
    init_NumberRange();

    init_StringAsciiRandom();
    init_StringLengthAsciiRandom();
    init_StringCaseLower();
    init_StringCaseRandom();
    init_StringCaseUpper();
    init_StringLengthEdgeCase();
    init_StringLengthRandom();
    init_StringLengthGauss();
    init_StringUtf8Bom();
    init_StringUtf8BomLength();
    init_StringUtf8BomStatic();
    init_StringStatic();
    init_StringMagic();
    init_StringEnum();

    init_Ipv4();
    init_Ipv6();
    init_Mac();

    // init_AFL();

    //如果用户自定义变异算法,写在这里

    //注册一个子定义的fill arg变异算法，供测试
    register_Mutater_fill_arg(ARG_CPU, get_cpu1);

    //覆盖率反馈模块llvm初始化
    init_pc_counters();
    init_8bit_counters();
    init_kcov();
    init_llvmdata();
    init_Corpus();
    init_SignalCallback();
}

void CLEAR_Common(void)
{

    addWholeRandomNum = 0;
    addWholeSequenceNum = 0;
    tempSequenceStartPos = 0;
}
