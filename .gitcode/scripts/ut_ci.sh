echo 'hello'

case "${codeBranch}" in
    master|br_bk26_930|dev_urma_20221104)

         ;;

    *)
        echo "分支名：${codeBranch}"

        bash run_coverage.sh ${codeBranch} 60
       ;;
esac
