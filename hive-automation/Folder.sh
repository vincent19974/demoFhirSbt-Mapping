cd
dir=$(pwd)
s=$dir/tmp/input

echo "$dir"
echo "$s"

if [ "$s" = "$dir" ]; then
        echo "Ok"
else
if [ -d  "$dir/tmp" ] 
then
    echo "Directory $dir/tmp exists." 
    if [ -d  "$dir/tmp/input" ] 
        then
            echo "Directory $dir/tmp/input exists." 
        else
            echo "Error: Directory $dir/tmp/input does not exists."
            mkdir $dir/tmp/input
    fi
    if [ -d  "$dir/tmp/output" ] 
        then
        echo "Directory $dir/tmp exists." 
        else
        echo "Error: Directory $dir/tmp/output does not exists."
        mkdir $dir/tmp/output
    fi
    if [ -d  "$dir/tmp/hql-input" ] 
        then
        echo "Directory $dir/tmp/hql-input exists." 
    else
        echo "Error: Directory $dir/tmp/hql-input does not exists."
        mkdir $dir/tmp/hql-input
    fi
    if [ -d  "$dir/tmp/hql-output" ] 
        then
        echo "Directory $dir/tmp/hql-output exists." 
    else
        echo "Error: Directory $dir/tmp/hql-output does not exists."
        mkdir $dir/tmp/hql-output
    fi

else
    echo "Error: Directory $dir/tmp does not exists."
    mkdir $dir/tmp
    if [ -d  "$dir/tmp/input" ] 
        then
            echo "Directory $dir/tmp/input exists." 
        else
            echo "Error: Directory $dir/tmp/input does not exists."
            mkdir $dir/tmp/input
    fi
    if [ -d  "$dir/tmp/output" ] 
        then
        echo "Directory $dir/tmp exists." 
        else
        echo "Error: Directory $dir/tmp/output does not exists."
        mkdir $dir/tmp/output
    fi
    if [ -d  "$dir/tmp/hql-input" ] 
        then
        echo "Directory $dir/tmp/hql-input exists." 
    else
        echo "Error: Directory $dir/tmp/hql-input does not exists."
        mkdir $dir/tmp/hql-input
    fi
    if [ -d  "$dir/tmp/hql-output" ] 
        then
        echo "Directory $dir/tmp/hql-output exists." 
    else
        echo "Error: Directory $dir/tmp/hql-output does not exists."
        mkdir $dir/tmp/hql-output
    fi
fi
fi
echo "You are not in the $dir/tmp/input folder?"
exit