/*
 * Copyright (c) 2025 OceanBase.
 * ... License Header ...
 */

#define USING_LOG_PREFIX SQL_ENG

#include "sql/engine/expr/ob_expr_token_hash.h"
#include "sql/engine/expr/ob_expr_lob_utils.h"
#include "share/ob_fts_index_builder_util.h"

using namespace oceanbase::common;

namespace oceanbase
{
namespace sql
{

ObExprTokenHash::ObExprTokenHash(ObIAllocator &alloc)
    : ObFuncExprOperator(alloc, T_FUN_SYS_TOKEN_HASH, N_TOKEN_HASH, 1, VALID_FOR_GENERATED_COL, NOT_ROW_DIMENSION)
{
}

ObExprTokenHash::~ObExprTokenHash()
{
}

int ObExprTokenHash::calc_result_type1(ObExprResType &type,
                                       ObExprResType &type1,
                                       common::ObExprTypeCtx &type_ctx) const
{
  int ret = OB_SUCCESS;
  type1.set_calc_type(common::ObVarcharType);
  type1.set_calc_collation_type(type1.get_collation_type());
  type.set_uint64();
  type.set_scale(0);
  return ret;
}

int ObExprTokenHash::calc_result_typeN(ObExprResType &type,
                                       ObExprResType *types,
                                       int64_t param_num,
                                       common::ObExprTypeCtx &type_ctx) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(param_num < 1) || OB_ISNULL(types)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument for token hash expr", K(ret), K(param_num), KP(types));
  } else {
    types[0].set_calc_type(common::ObVarcharType);
    types[0].set_calc_collation_type(types[0].get_collation_type());
    type.set_uint64();
    type.set_scale(0);
  }
  return ret;
}

int ObExprTokenHash::calc_result1(common::ObObj &result,
                                  const common::ObObj &obj1,
                                  common::ObExprCtx &expr_ctx) const
{
  int ret = OB_SUCCESS;
  if (obj1.is_null()) {
    result.set_uint64(0);
  } else {
    ObString text = obj1.get_string();
    uint64_t hash_val = murmurhash64A(text.ptr(), text.length(), 0);
    result.set_uint64(hash_val);
  }
  return ret;
}

int ObExprTokenHash::calc_resultN(common::ObObj &result,
                                  const common::ObObj *objs_array,
                                  int64_t param_num,
                                  common::ObExprCtx &expr_ctx) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(param_num < 1) || OB_ISNULL(objs_array)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret));
  } else {
    const ObObj &input = objs_array[0];
    if (input.is_null()) {
      result.set_uint64(0);
    } else {
      ObString text = input.get_string();
      uint64_t hash_val = murmurhash64A(text.ptr(), text.length(), 0);
      result.set_uint64(hash_val);
    }
  }
  return ret;
}

int ObExprTokenHash::cg_expr(ObExprCGCtx &cg_ctx,
                             const ObRawExpr &raw_expr,
                             ObExpr &rt_expr) const
{
  int ret = OB_SUCCESS;
  UNUSED(raw_expr);
  UNUSED(cg_ctx);
  if (OB_UNLIKELY(rt_expr.arg_cnt_ < 1) || OB_ISNULL(rt_expr.args_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(rt_expr.arg_cnt_), KP(rt_expr.args_));
  } else {
    rt_expr.eval_func_ = eval_token_hash;
  }
  return ret;
}

/*static*/ int ObExprTokenHash::eval_token_hash(const ObExpr &raw_ctx,
                                                ObEvalCtx &eval_ctx,
                                                ObDatum &expr_datum)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(raw_ctx.arg_cnt_ <= 0) || OB_ISNULL(raw_ctx.args_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(raw_ctx));
  } else {
    ObEvalCtx::TempAllocGuard alloc_guard(eval_ctx);
    ObDatum *arg_datum = nullptr;
    ObString res_str;
    if (OB_FAIL(raw_ctx.args_[0]->eval(eval_ctx, arg_datum))) {
       LOG_WARN("eval arg failed", K(ret));
    } else if (OB_ISNULL(arg_datum)) {
       ret = OB_ERR_UNEXPECTED;
       LOG_WARN("datum is null", K(ret));
    } else if (arg_datum->is_null()) {
       expr_datum.set_uint(0);
    } else {
       res_str = arg_datum->get_string();
       if (OB_FAIL(ObTextStringHelper::read_real_string_data(
             alloc_guard.get_allocator(), 
             *arg_datum, 
             raw_ctx.args_[0]->datum_meta_, 
             raw_ctx.args_[0]->obj_meta_.has_lob_header(), 
             res_str))) {
           LOG_WARN("fail to get real data", K(ret));
       } else {
           uint64_t hash_val = 0;
           if (res_str.length() > 0) {
               hash_val = murmurhash64A(res_str.ptr(), res_str.length(), 0);
           }
           expr_datum.set_uint(hash_val);
           LOG_DEBUG("eval token hash", K(res_str), K(hash_val));
       }
    }
  }
  return ret;
}

} // namespace sql
} // namespace oceanbase